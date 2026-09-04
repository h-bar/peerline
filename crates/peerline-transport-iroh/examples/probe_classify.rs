//! What does `endpoint.connect` report when the RELAY itself is
//! unreachable (closed port / RST / blackhole)? Mirrors the plugin's
//! `probe_path` classification to show whether such a failure would be
//! reported as a (false) "reachable" by the connectivity probe.
//!
//! Run: cargo run --example probe_classify -- <relay-url>...

use std::time::{Duration, Instant};

use iroh::Endpoint;
use iroh::endpoint::presets;
use iroh_base::{EndpointAddr, SecretKey};
use peerline_transport_iroh::IrohConfig;

const PROBE_ALPN: &[u8] = b"peerline/probe/1";

#[tokio::main]
async fn main() {
    let urls: Vec<String> = std::env::args().skip(1).collect();
    let urls = if urls.is_empty() {
        vec![
            "http://127.0.0.1:9".to_string(),        // closed port -> RST
            "http://192.0.2.1:3340".to_string(),     // blackhole -> timeout
            "http://106.15.43.194:9999".to_string(), // real host, closed port
        ]
    } else {
        urls
    };

    // A random peer id nobody serves — the probe never reaches a peer, the
    // question is only how the RELAY failure is classified.
    let fake_peer = SecretKey::generate().public();

    for url in urls {
        let cfg = IrohConfig::from_relay_urls([url.as_str()]).expect("relay url");
        let relay = cfg.relays[0].clone();
        let addr = EndpointAddr::from(fake_peer).with_relay_url(relay);

        let mut builder = Endpoint::builder(presets::N0);
        if let Some(mode) = cfg.relay_mode() {
            builder = builder.relay_mode(mode);
        }
        let ep = builder.bind().await.expect("bind");
        let t0 = Instant::now();
        let outcome =
            tokio::time::timeout(Duration::from_secs(12), ep.connect(addr, PROBE_ALPN)).await;
        let ms = t0.elapsed().as_millis();
        let (verdict, detail) = match outcome {
            Ok(Ok(_)) => ("reachable (connected)", "connected".to_string()),
            Ok(Err(e)) => {
                let msg = e.to_string();
                let low = msg.to_lowercase();
                let dead = [
                    "timed out",
                    "timeout",
                    "no route",
                    "unreachable",
                    "no known",
                    "no working",
                    "no path",
                    "no addr",
                ]
                .iter()
                .any(|k| low.contains(k));
                if dead {
                    ("UNREACHABLE", msg)
                } else {
                    ("reachable (FALSE GREEN?)", msg)
                }
            }
            Err(_) => ("UNREACHABLE (probe timeout)", "timed out".into()),
        };
        println!("{url}\n   -> {verdict} after {ms}ms — error: {detail}\n");
        ep.close().await;
    }
}
