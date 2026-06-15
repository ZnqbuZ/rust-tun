use std::collections::VecDeque;
use std::env;
use std::io::{self, ErrorKind, IoSlice};
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::process::Command;
use std::task::{ready, Context, Poll};

use bytes::{Buf, Bytes, BytesMut};
use futures::{Sink, SinkExt, StreamExt};
use tokio::io::AsyncWrite;
use tokio::net::{TcpListener, TcpSocket};
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::io::poll_write_buf;
use tun_easytier::{AsyncReader, AsyncWriter, Configuration, AsyncReadExt, AsyncWriteExt, AbstractDevice};

const MAX_MTU: u16 = 1500;
const BUFFER_SIZE: usize = 65536;
const BATCH_SIZE: usize = 64;

// --- High-Performance Utilities ---

struct BufList {
    bufs: VecDeque<Bytes>,
}

impl BufList {
    fn new() -> Self {
        Self {
            bufs: VecDeque::new(),
        }
    }
    fn push(&mut self, buf: Bytes) {
        if buf.has_remaining() {
            self.bufs.push_back(buf);
        }
    }
    fn len(&self) -> usize {
        self.bufs.len()
    }
}

impl Buf for BufList {
    fn remaining(&self) -> usize {
        self.bufs.iter().map(|b| b.remaining()).sum()
    }
    fn chunk(&self) -> &[u8] {
        self.bufs.front().map(|b| b.chunk()).unwrap_or_default()
    }
    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let mut n = 0;
        for buf in &self.bufs {
            if n >= dst.len() {
                break;
            }
            n += buf.chunks_vectored(&mut dst[n..]);
        }
        n
    }
    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 {
            let front = self.bufs.front_mut().expect("advance beyond remaining");
            let rem = front.remaining();
            if cnt < rem {
                front.advance(cnt);
                return;
            } else {
                cnt -= rem;
                self.bufs.pop_front();
            }
        }
    }
}

struct PacketCodec;
impl Decoder for PacketCodec {
    type Item = Bytes;
    type Error = std::io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_le_bytes(src[..4].try_into().unwrap()) as usize;
        if len > BUFFER_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Packet too large",
            ));
        }
        if src.len() < 4 + len {
            src.reserve(4 + len - src.len());
            return Ok(None);
        }
        src.advance(4);
        Ok(Some(src.split_to(len).freeze()))
    }
}

struct BatchedFramedWriter<W> {
    writer: W,
    sending_bufs: BufList,
}

impl<W: AsyncWrite + Unpin> BatchedFramedWriter<W> {
    fn new(writer: W) -> Self {
        Self {
            writer,
            sending_bufs: BufList::new(),
        }
    }
}

impl<W: AsyncWrite + Unpin> Sink<Bytes> for BatchedFramedWriter<W> {
    type Error = std::io::Error;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.sending_bufs.len() >= BATCH_SIZE {
            Pin::new(this).poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        self.get_mut().sending_bufs.push(item);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        while this.sending_bufs.remaining() > 0 {
            let n = ready!(poll_write_buf(
                Pin::new(&mut this.writer),
                cx,
                &mut this.sending_bufs
            ))?;
            if n == 0 {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "TCP closed",
                )));
            }
        }
        Pin::new(&mut this.writer).poll_flush(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;
        Pin::new(&mut self.get_mut().writer).poll_shutdown(cx)
    }
}

// --- TunRx and TunTx from easytier ---

struct TunRx {
    reader: AsyncReader,
    buf: BytesMut,
}

impl TunRx {
    fn new(reader: AsyncReader) -> Self {
        Self {
            reader,
            buf: BytesMut::with_capacity(BUFFER_SIZE),
        }
    }

    async fn recv(&mut self) -> io::Result<Bytes> {
        self.buf.reserve(BUFFER_SIZE);
        // Leave 4 bytes for TCP length prefix
        unsafe {
            self.buf.set_len(BUFFER_SIZE + 4);
        }

        let n = self.reader.read(&mut self.buf[4..]).await?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "read zero"));
        }

        self.buf.truncate(n + 4);

        let len_bytes = (n as u32).to_le_bytes();
        self.buf[0..4].copy_from_slice(&len_bytes);

        Ok(self.buf.split_to(n + 4).freeze())
    }
}

struct TunTx {
    writer: AsyncWriter,
    pub dropped: u64,
}

impl TunTx {
    fn new(writer: AsyncWriter) -> Self {
        Self { writer, dropped: 0 }
    }

    fn write(&mut self, frame: &[u8]) -> io::Result<()> {
        match self.writer.try_write(frame) {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.kind() != ErrorKind::WouldBlock {
                    return Err(e);
                }
                self.dropped += 1;
                Ok(())
            }
        }
    }

    pub fn send(&mut self, item: Bytes) -> io::Result<()> {
        // item already has length prefix stripped by FramedRead
        self.write(&item)
    }
}

// --- Tasks ---

async fn tun_to_tcp_task(
    reader: tun_easytier::AsyncReader,
    tcp_write: tokio::net::tcp::OwnedWriteHalf,
) -> Result<(), std::io::Error> {
    let mut batched_write = BatchedFramedWriter::new(tcp_write);
    let mut tun_rx = TunRx::new(reader);

    loop {
        let pkt = tun_rx.recv().await?;
        batched_write.feed(pkt).await?;
        // If we want to flush immediately for latency we can do it,
        // but batching helps with throughput. Let's flush every packet
        // for simplicity, or rely on another mechanism. Actually, let's
        // just yield to let batching build up naturally.
        batched_write.flush().await?;
    }
}

async fn tcp_to_tun_task(
    tcp_read: tokio::net::tcp::OwnedReadHalf,
    writer: tun_easytier::AsyncWriter,
) -> Result<(), std::io::Error> {
    let mut framed_read = FramedRead::new(tcp_read, PacketCodec);
    let mut tun_tx = TunTx::new(writer);

    while let Some(res) = framed_read.next().await {
        let pkt = res?;
        if let Err(e) = tun_tx.send(pkt) {
            eprintln!("tun tx error: {:?}", e);
        }
    }
    Ok(())
}

fn setup_route(tun_name: &str, target_ip: &str) {
    println!(
        "Setting up route: ip route add {} dev {}",
        target_ip, tun_name
    );
    let _ = Command::new("ip")
        .args(["route", "add", target_ip, "dev", tun_name])
        .status();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: {} <client|server> <addr> <tun_ip>", args[0]);
        return Ok(());
    }

    let mode = &args[1];
    let addr: SocketAddr = args[2].parse()?;
    let tun_ip = args[3].clone();
    let target_ip = if mode == "server" {
        "172.17.0.1"
    } else {
        "172.17.0.2"
    };

    let mut config = Configuration::default();
    config
        .address(tun_ip.parse::<Ipv4Addr>()?)
        .netmask((255, 255, 255, 0))
        .mtu(MAX_MTU)
        .tun_name("tun0")
        .up();

    #[cfg(target_os = "linux")]
    config.platform_config(|config| {
        config.ensure_root_privileges(true);
        config.vnet_hdr(false); // No VNET_HDR
    });

    let dev = tun_easytier::create_as_async(&config)?;
    let tun_name = dev.tun_name()?;
    println!("TUN device created: {}", tun_name);

    setup_route(&tun_name, target_ip);

    let (reader, writer) = dev.split();

    let stream = if mode == "server" {
        let listener = TcpListener::bind(addr).await?;
        println!("TCP Server listening on {}", addr);
        let (stream, peer) = listener.accept().await?;
        println!("Accepted connection from {}", peer);
        stream
    } else {
        println!("Connecting to TCP server at {}", addr);
        let socket = if addr.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        let _ = socket.set_send_buffer_size(1024 * 1024);
        let _ = socket.set_recv_buffer_size(1024 * 1024);
        socket.connect(addr).await?
    };

    stream.set_nodelay(true)?;
    let (tcp_read, tcp_write) = stream.into_split();

    println!(
        "Extreme performance tunnel established between {} and {}",
        tun_ip, target_ip
    );

    let t1 = tokio::spawn(tun_to_tcp_task(reader, tcp_write));
    let t2 = tokio::spawn(tcp_to_tun_task(tcp_read, writer));

    tokio::select! {
        res = t1 => { println!("TUN -> TCP task finished: {:?}", res?); }
        res = t2 => { println!("TCP -> TUN task finished: {:?}", res?); }
    }

    Ok(())
}
