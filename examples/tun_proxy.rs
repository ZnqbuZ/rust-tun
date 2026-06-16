use std::env;
use std::io::{self, ErrorKind};
use std::net::{Ipv4Addr, SocketAddr};
use std::process::Command;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tun_rs::{AsyncDevice, DeviceBuilder};

const MAX_MTU: u16 = 1400;
const BUFFER_SIZE: usize = 65536;

async fn tun_to_udp_task(
    dev: Arc<AsyncDevice>,
    socket: Arc<UdpSocket>,
) -> Result<(), std::io::Error> {
    let mut buf = vec![0u8; BUFFER_SIZE];
    loop {
        let n = dev.recv(&mut buf).await?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "TUN device closed"));
        }
        socket.send(&buf[..n]).await?;
    }
}

async fn udp_to_tun_task(
    socket: Arc<UdpSocket>,
    dev: Arc<AsyncDevice>,
    mut first_pkt: Option<Vec<u8>>,
) -> Result<(), std::io::Error> {
    // 优先处理服务端在建立连接时缓存的首个数据包
    if let Some(pkt) = first_pkt.take() {
        if let Err(e) = dev.try_send(&pkt) {
            eprintln!("TUN tx error for first packet: {:?}", e);
        }
    }

    let mut buf = vec![0u8; BUFFER_SIZE];
    loop {
        let n = socket.recv(&mut buf).await?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "UDP socket closed"));
        }
        if let Err(e) = dev.try_send(&buf[..n]) {
            if e.kind() != ErrorKind::WouldBlock {
                eprintln!("TUN tx error: {:?}", e);
            }
        }
    }
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

    let dev = Arc::new(DeviceBuilder::new()
        .name("tun0")
        .ipv4(tun_ip.parse::<Ipv4Addr>()?, 24, None)
        .mtu(MAX_MTU)
        .build_async()?);

    let tun_name = dev.name()?;
    println!("TUN device created: {}", tun_name);

    setup_route(&tun_name, target_ip);

    let (socket, first_pkt) = if mode == "server" {
        let socket = UdpSocket::bind(addr).await?;
        println!("UDP Server listening on {}. Waiting for client packet...", addr);

        let mut first_buf = vec![0u8; BUFFER_SIZE];
        let (n, peer) = socket.recv_from(&mut first_buf).await?;
        println!("Received packet from peer: {}, establishing point-to-point tunnel", peer);

        socket.connect(peer).await?;
        (Arc::new(socket), Some(first_buf[..n].to_vec()))
    } else {
        let local_addr: SocketAddr = if addr.is_ipv4() {
            "0.0.0.0:0".parse()?
        } else {
            "[::]:0".parse()?
        };
        let socket = UdpSocket::bind(local_addr).await?;
        println!("Connecting to UDP server at {}", addr);
        socket.connect(addr).await?;
        (Arc::new(socket), None)
    };

    println!(
        "UDP tunnel established between {} and {}",
        tun_ip, target_ip
    );

    let t1 = tokio::spawn(tun_to_udp_task(dev.clone(), socket.clone()));
    let t2 = tokio::spawn(udp_to_tun_task(socket.clone(), dev.clone(), first_pkt));

    tokio::select! {
        res = t1 => { println!("TUN -> UDP task finished: {:?}", res?); }
        res = t2 => { println!("UDP -> TUN task finished: {:?}", res?); }
    }

    Ok(())
}