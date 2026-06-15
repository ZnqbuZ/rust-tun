//            DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
//                    Version 2, December 2004
//
// Copyleft (ↄ) meh. <meh@schizofreni.co> | http://meh.schizofreni.co
//
// Everyone is permitted to copy and distribute verbatim or modified
// copies of this license document, and changing it is allowed as long
// as the name is changed.
//
//            DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
//   TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION
//
//  0. You just DO WHAT THE FUCK YOU WANT TO.

use std::collections::VecDeque;
use std::env;
use std::io::IoSlice;
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::process::Command;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use futures::{Sink, SinkExt, StreamExt};
use tokio::io::AsyncWrite;
use tokio::net::{TcpListener, TcpSocket};
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::io::poll_write_buf;
use tun_easytier::{AbstractDevice, AsyncReadExt, AsyncWriteExt, Configuration};

const MAX_MTU: u16 = 65535;
const VNET_HDR_LEN: usize = 10;
const LEN_PREFIX_LEN: usize = 4;
const BUFFER_SIZE: usize = MAX_MTU as usize + VNET_HDR_LEN + LEN_PREFIX_LEN + 64;
const BATCH_SIZE: usize = 128;
const FLUSH_INTERVAL: Duration = Duration::from_millis(1);

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
        // Flush if batch size is reached
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

// --- Optimization: Zero-Copy Length Prefixing ---

async fn tun_to_tcp_task(
    mut reader: tun_easytier::AsyncReader,
    tcp_write: tokio::net::tcp::OwnedWriteHalf,
) -> Result<(), std::io::Error> {
    let mut batched_write = BatchedFramedWriter::new(tcp_write);
    let mut interval = tokio::time::interval(FLUSH_INTERVAL);
    let mut pool = BytesMut::with_capacity(BUFFER_SIZE);

    loop {
        tokio::select! {
            n_res = reader.read(unsafe {
                // Read into buffer starting at offset 4 to leave space for length prefix
                pool.set_len(BUFFER_SIZE);
                &mut pool[LEN_PREFIX_LEN..]
            }) => {
                let n = n_res?;
                if n == 0 { break; }

                // Write length prefix into the first 4 bytes
                let len_bytes = (n as u32).to_le_bytes();
                pool[0..4].copy_from_slice(&len_bytes);

                // Extract the whole packet (4 bytes prefix + n bytes data)
                let pkt = pool.split_to(n + LEN_PREFIX_LEN).freeze();
                batched_write.feed(pkt).await?;
            }
            _ = interval.tick() => {
                batched_write.flush().await?;
            }
        }
    }
    Ok(())
}

async fn tcp_to_tun_task(
    tcp_read: tokio::net::tcp::OwnedReadHalf,
    mut writer: tun_easytier::AsyncWriter,
) -> Result<(), std::io::Error> {
    let mut framed_read = FramedRead::new(tcp_read, PacketCodec);
    while let Some(res) = framed_read.next().await {
        let pkt = res?;
        writer.write(&pkt).await?;
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
        .tun_name("tun0") // Fixed name for easier routing
        .up();

    #[cfg(target_os = "linux")]
    config.platform_config(|config| {
        config.ensure_root_privileges(true);
        config.vnet_hdr(true);
    });

    let dev = tun_easytier::create_as_async(&config)?;
    let tun_name = dev.tun_name()?;
    println!("TUN device created: {}", tun_name);

    // Auto-setup route (optional, but helps avoid user error)
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
        // Increase TCP buffer sizes
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
