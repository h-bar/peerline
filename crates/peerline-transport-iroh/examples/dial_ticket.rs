//! Dial a real peerline ticket in three shapes — full addr, relay-only,
//! directs-only — with a caller-supplied ALPN, reporting per-shape
//! outcome + timing. Diagnoses "probe green but connect fails" cases.
//!
//! Run: cargo run --example dial_ticket -- <ticket> <alpn> [shape]

use std::time::{Duration, Instant};

use iroh::Endpoint;
use iroh::endpoint::presets;
use iroh_base::EndpointAddr;
use peerline_transport_iroh::{IrohConfig, decode_ticket};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let ticket = args
        .next()
        .expect("usage: dial_ticket <ticket> <alpn> [full|relay|direct]");
    let alpn = args
        .next()
        .expect("usage: dial_ticket <ticket> <alpn> [full|relay|direct]");
    let shape = args.next().unwrap_or_else(|| "all".into());

    let addr = decode_ticket(&ticket).expect("decode ticket");
    let relays: Vec<_> = addr.relay_urls().cloned().collect();
    let directs: Vec<_> = addr.ip_addrs().copied().collect();
    println!("ticket id={}", addr.id);
    println!("  relays:  {relays:?}");
    println!("  directs: {directs:?}");

    let relay_only = relays.iter().fold(EndpointAddr::from(addr.id), |a, r| {
        a.with_relay_url(r.clone())
    });
    let direct_only = directs
        .iter()
        .fold(EndpointAddr::from(addr.id), |a, i| a.with_ip_addr(*i));

    let cfg = IrohConfig {
        relays: relays.clone(),
        ..Default::default()
    };
    if shape == "all" || shape == "full" {
        dial(&cfg, "full", addr.clone(), alpn.as_bytes()).await;
    }
    if shape == "all" || shape == "relay" {
        dial(&cfg, "relay-only", relay_only.clone(), alpn.as_bytes()).await;
    }
    if shape == "relay-bench" {
        // Throughput through the relay: strict relay endpoint, real ALPN,
        // send one `list` request and time the full framed response.
        let mut builder = iroh::Endpoint::builder(presets::Minimal).clear_ip_transports();
        if let Some(mode) = cfg.relay_mode() {
            builder = builder.relay_mode(mode);
        }
        let ep = builder.bind().await.expect("bind (relay bench)");
        let conn = ep
            .connect(relay_only.clone(), alpn.as_bytes())
            .await
            .expect("relay bench connect");
        let (send, recv) = conn.open_bi().await.expect("open_bi");
        let (mut sink, mut stream) =
            peerline_transport_iroh::text_frames(tokio::io::join(recv, send));
        use futures::{SinkExt, StreamExt};
        let t0 = std::time::Instant::now();
        sink.send(r#"{"ver":"1","kind":"req","id":1,"op":"list","args":{}}"#.to_string())
            .await
            .expect("send list");
        let resp = tokio::time::timeout(Duration::from_secs(60), stream.next())
            .await
            .expect("response within 60s")
            .expect("stream open")
            .expect("frame");
        let ms = t0.elapsed().as_millis();
        println!(
            "== relay-bench: {} bytes in {ms}ms ({:.0} KB/s)",
            resp.len(),
            resp.len() as f64 / 1024.0 / (ms as f64 / 1000.0)
        );
        ep.close().await;
    }
    if shape == "relay-strict" {
        // Relay-only addr on an endpoint with NO IP transports: holepunching
        // has no socket to open a direct path with, so the connection can
        // never upgrade off the relay (how iroh runs in browsers).
        let mut builder = iroh::Endpoint::builder(presets::Minimal).clear_ip_transports();
        if let Some(mode) = cfg.relay_mode() {
            builder = builder.relay_mode(mode);
        }
        let ep = builder.bind().await.expect("bind (no ip transports)");
        let t0 = std::time::Instant::now();
        match tokio::time::timeout(
            Duration::from_secs(20),
            ep.connect(relay_only, alpn.as_bytes()),
        )
        .await
        {
            Ok(Ok(conn)) => {
                println!("== relay-strict: QUIC OK in {:?}", t0.elapsed());
                // Give holepunching time to (fail to) upgrade, then inspect.
                tokio::time::sleep(Duration::from_secs(5)).await;
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
                println!("   paths after 5s: {paths:?}");
            }
            Ok(Err(e)) => println!("== relay-strict FAILED in {:?}: {e}", t0.elapsed()),
            Err(_) => println!("== relay-strict TIMED OUT"),
        }
        ep.close().await;
    }
    if shape == "all" || shape == "direct" {
        dial(
            &IrohConfig::default(),
            "direct-only",
            direct_only,
            alpn.as_bytes(),
        )
        .await;
    }
    if shape == "all" || shape == "hostile" {
        // Off-LAN simulation: the ticket's direct addrs, on a foreign
        // network, can hit hosts that ACTIVELY REFUSE (ICMP port
        // unreachable) instead of blackholing — e.g. the local router at a
        // clashing RFC1918 addr, or a proxy TUN at 198.18.0.1. Dial the
        // relay plus such refusing addrs only (no reachable directs).
        let refusing: Vec<std::net::SocketAddr> = vec![
            "127.0.0.1:9".parse().unwrap(),
            "192.168.31.1:6466".parse().unwrap(), // this LAN's router, closed port
        ];
        let mut hostile = relays.iter().fold(EndpointAddr::from(addr.id), |a, r| {
            a.with_relay_url(r.clone())
        });
        for i in refusing {
            hostile = hostile.with_ip_addr(i);
        }
        dial(&cfg, "relay+refusing-directs", hostile, alpn.as_bytes()).await;
    }
}

async fn dial(cfg: &IrohConfig, label: &str, addr: EndpointAddr, alpn: &[u8]) {
    println!("== dial {label}");
    let mut builder = Endpoint::builder(presets::N0);
    if let Some(mode) = cfg.relay_mode() {
        builder = builder.relay_mode(mode);
    }
    let ep = builder.bind().await.expect("client bind");
    let t0 = Instant::now();
    match tokio::time::timeout(Duration::from_secs(20), ep.connect(addr, alpn)).await {
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
            println!("   QUIC OK in {:?} — paths: {paths:?}", t0.elapsed());
            // Exercise the app layer too: open the bi-stream like the
            // plugin does. accept_bi on the far side needs a first frame,
            // so just opening must succeed instantly.
            match conn.open_bi().await {
                Ok(_) => println!("   open_bi OK"),
                Err(e) => println!("   open_bi FAILED: {e}"),
            }
        }
        Ok(Err(e)) => println!("   FAILED in {:?}: {e}", t0.elapsed()),
        Err(_) => println!("   TIMED OUT after {:?}", t0.elapsed()),
    }
    ep.close().await;
}
