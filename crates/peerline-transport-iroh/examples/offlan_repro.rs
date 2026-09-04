//! Repro for the off-LAN dial failure: an acceptor homed on the custom
//! relay is dialed (1) relay-only and (2) relay + blackholed direct
//! addresses — the exact addr an off-LAN client sees after decoding a
//! ticket whose direct addrs are another network's LAN IPs.
//!
//! Run: cargo run --example offlan_repro [relay-url]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use iroh::Endpoint;
use iroh::endpoint::presets;
use iroh_base::{EndpointAddr, SecretKey};
use peerline_transport_iroh::IrohConfig;

const ALPN: &[u8] = b"peerline/repro/1";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let relay = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://106.15.43.194:3340".to_string());
    let cfg = IrohConfig::from_relay_urls([relay.as_str()]).expect("relay url");

    // Acceptor: homed on the custom relay, serving ALPN.
    let mut builder = Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .secret_key(SecretKey::generate());
    if let Some(mode) = cfg.relay_mode() {
        builder = builder.relay_mode(mode);
    }
    let server = builder.bind().await.expect("server bind");
    let _ = tokio::time::timeout(Duration::from_secs(10), server.online()).await;
    let addr = server.addr();
    let relays: Vec<_> = addr.relay_urls().cloned().collect();
    let directs: Vec<_> = addr.ip_addrs().copied().collect();
    println!(
        "server id={} relays={relays:?} directs={directs:?}",
        addr.id
    );
    assert!(
        !relays.is_empty(),
        "server acquired no relay — is the relay up?"
    );

    let server_task = tokio::spawn({
        let server = server.clone();
        async move {
            while let Some(incoming) = server.accept().await {
                tokio::spawn(async move {
                    match incoming.await {
                        Ok(conn) => {
                            println!("  [server] accepted conn from {}", conn.remote_id());
                            // hold it open a bit
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                        Err(e) => println!("  [server] incoming failed: {e}"),
                    }
                });
            }
        }
    });

    // Case 1: relay-only addr (what the probe dials).
    let relay_only = relays.iter().fold(EndpointAddr::from(addr.id), |a, r| {
        a.with_relay_url(r.clone())
    });
    dial(&cfg, "relay-only", relay_only).await;

    // Case 2: relay + blackholed direct addrs (what an off-LAN client
    // effectively dials: the ticket's LAN IPs route nowhere).
    let blackholes: Vec<SocketAddr> = vec![
        "192.0.2.1:41234".parse().unwrap(),
        "192.0.2.2:41234".parse().unwrap(),
    ];
    let mut mixed = relays.iter().fold(EndpointAddr::from(addr.id), |a, r| {
        a.with_relay_url(r.clone())
    });
    for b in blackholes {
        mixed = mixed.with_ip_addr(b);
    }
    dial(&cfg, "relay+blackholed-directs", mixed).await;

    server_task.abort();
}

async fn dial(cfg: &IrohConfig, label: &str, addr: EndpointAddr) {
    println!("== dial {label}: {:?}", addr);
    let mut builder = Endpoint::builder(presets::N0);
    if let Some(mode) = cfg.relay_mode() {
        builder = builder.relay_mode(mode);
    }
    let ep = builder.bind().await.expect("client bind");
    let t0 = Instant::now();
    match tokio::time::timeout(Duration::from_secs(20), ep.connect(addr, ALPN)).await {
        Ok(Ok(conn)) => {
            let paths: Vec<String> = conn
                .paths()
                .iter()
                .map(|p| {
                    format!(
                        "{}{}",
                        p.remote_addr(),
                        if p.is_selected() { " [selected]" } else { "" }
                    )
                })
                .collect();
            println!("   OK in {:?} — paths: {paths:?}", t0.elapsed());
        }
        Ok(Err(e)) => println!("   FAILED in {:?}: {e}", t0.elapsed()),
        Err(_) => println!("   TIMED OUT after {:?}", t0.elapsed()),
    }
    ep.close().await;
}
