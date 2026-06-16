use std::env;
use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::RwLock;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tun_rs::{AsyncDevice, DeviceBuilder};

const MAX_MTU: u16 = 1400;
const BUFFER_SIZE: usize = 65536;
const QUEUE_COUNT: usize = 4;

// 引入全局共享的动态 Peer 地址
type SharedPeer = Arc<RwLock<Option<SocketAddr>>>;

async fn queue_worker(
    queue_id: usize,
    dev: Arc<AsyncDevice>,
    socket: Arc<UdpSocket>,
    shared_peer: SharedPeer,
    is_client: bool,
) -> Result<(), std::io::Error> {
    println!("Worker [{}] started.", queue_id);

    let mut rx_buf = vec![0u8; BUFFER_SIZE];
    let mut tx_buf = vec![0u8; BUFFER_SIZE];

    // 终极优化：线程本地变量，彻底消除 Hot Path 上的原子锁争用
    let mut local_peer: Option<SocketAddr> = None;

    loop {
        tokio::select! {
            // 1. 从 TUN 读取，通过 UDP 发送
            res = dev.recv(&mut rx_buf) => {
                let n = res?;
                if n == 0 { break; }

                // Fast Path：无锁读取本地变量（零成本）
                if local_peer.is_none() {
                    // Slow Path：仅在本地不知道发给谁时，才去读全局锁
                    local_peer = *shared_peer.read().unwrap();
                }

                if let Some(addr) = local_peer {
                    let _ = socket.send_to(&rx_buf[..n], addr).await;
                }
            }

            // 2. 从 UDP 读取，写入 TUN，并维护地址状态
            res = socket.recv_from(&mut tx_buf) => {
                let (n, peer_addr) = res?;
                if n == 0 { break; }

                // 仅当物理对端地址发生实质变化时，才触发状态更新
                if local_peer != Some(peer_addr) {
                    // 更新当前线程的无锁缓存
                    local_peer = Some(peer_addr);

                    if !is_client {
                        // 严格读写分离：先用无锁的 read() 检查全局状态
                        let global_val = *shared_peer.read().unwrap();
                        if global_val != Some(peer_addr) {
                            println!("Worker [{}] globally mapped to new peer: {}", queue_id, peer_addr);
                            // 只有全局状态真的过时了，才去抢占极其昂贵的写锁
                            *shared_peer.write().unwrap() = Some(peer_addr);
                        }
                    }
                }

                if let Err(e) = dev.send(&tx_buf[..n]).await {
                    if e.kind() != ErrorKind::WouldBlock {
                        eprintln!("Worker [{}] TUN tx error: {:?}", queue_id, e);
                    }
                }
            }
        }
    }
    Ok(())
}

fn create_reuseport_udp(addr: SocketAddr) -> Result<std::net::UdpSocket, std::io::Error> {
    let domain = if addr.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;

    Ok(socket.into())
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

    let dev_main = DeviceBuilder::new()
        .name("tun0")
        .ipv4(tun_ip.parse::<Ipv4Addr>()?, 24, None)
        .mtu(MAX_MTU)
        .multi_queue(true)
        .build_async()?;

    let mut devices = vec![dev_main];
    for _ in 1..QUEUE_COUNT {
        devices.push(devices[0].try_clone()?);
    }

    // 初始化全局对端地址表
    let shared_peer: SharedPeer = Arc::new(RwLock::new(None));

    if mode == "client" {
        // 客户端明确知道服务端的地址，直接预先填入
        *shared_peer.write().unwrap() = Some(addr);
    }

    let mut handles = vec![];

    for (i, dev) in devices.into_iter().enumerate() {
        let dev_arc = Arc::new(dev);
        let peer_clone = shared_peer.clone();

        let socket = if mode == "server" {
            let std_sock = create_reuseport_udp(addr)?;
            UdpSocket::from_std(std_sock)?
        } else {
            let local_addr: SocketAddr = "0.0.0.0:0".parse()?;
            let std_sock = create_reuseport_udp(local_addr)?;
            UdpSocket::from_std(std_sock)?
            // 客户端不再使用 connect()，而是统一使用 send_to() 以保持逻辑对称
        };

        let socket_arc = Arc::new(socket);

        let is_client = mode == "client";
        handles.push(tokio::spawn(async move {
            if let Err(e) = queue_worker(i, dev_arc, socket_arc, peer_clone, is_client).await {
                eprintln!("Worker [{}] error: {:?}", i, e);
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}