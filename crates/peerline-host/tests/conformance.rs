//! One conformance battery, run against every transport (and the host).
//!
//! Each transport is wrapped in a small [`Harness`] that stands up a server
//! registering the same [`register`] handler-set and hands back an
//! [`Endpoint`] you can dial. The identical [`battery`] then exercises the
//! full peerline surface over that connection — so any behavioural drift
//! between transports fails a test.
//!
//! The `perf` test (ignored by default) reuses the same harnesses to run
//! identical workloads across transports and prints a comparison table:
//!
//! ```text
//! cargo test -p peerline-host --release -- --ignored --nocapture perf
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::stream::StreamExt;
use peerline::runtime::{Error, Peer, StreamReceiver};
use peerline::wire::RpcError;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Concurrent in-flight requests used by the multiplexing test.
const CONCURRENCY: usize = 8;

type PeerHandler = Arc<dyn Fn(&Peer) + Send + Sync + 'static>;
type Connected = (Peer, BoxFuture<'static, ()>);

#[derive(Serialize, Deserialize)]
struct Echo {
    s: String,
}
#[derive(Serialize, Deserialize)]
struct Add {
    a: i64,
    b: i64,
}
#[derive(Serialize, Deserialize)]
struct N {
    n: u32,
}
#[derive(Serialize, Deserialize)]
struct Idx {
    i: u32,
}

fn rpc_err(e: impl std::fmt::Display) -> RpcError {
    RpcError { code: -32000, message: e.to_string(), data: None }
}

/// The server-side handler set every harness registers per connection.
fn register(peer: &Peer) {
    peer.on_request("echo", |e: Echo| async move { Ok::<_, RpcError>(e.s) });
    peer.on_request("add", |a: Add| async move { Ok::<_, RpcError>(a.a + a.b) });
    peer.on_request("fail", |_: serde_json::Value| async move {
        Err::<(), _>(RpcError { code: -32050, message: "boom".into(), data: None })
    });

    // Notification sink + a request that reads the accumulated count.
    let counter = Arc::new(AtomicI64::new(0));
    let c = counter.clone();
    peer.on_notification("note", move |n: N| {
        let c = c.clone();
        async move {
            c.fetch_add(n.n as i64, Ordering::SeqCst);
        }
    });
    let c = counter.clone();
    peer.on_request("count", move |_: serde_json::Value| {
        let c = c.clone();
        async move { Ok::<_, RpcError>(c.load(Ordering::SeqCst)) }
    });

    // Streaming: N items, then a stream that errors mid-flight.
    peer.on_stream_request("count_stream", |q: N, sender| async move {
        for i in 0..q.n {
            sender.send_item(&i).map_err(rpc_err)?;
        }
        Ok(())
    });
    peer.on_stream_request("boom_stream", |_: serde_json::Value, sender| async move {
        sender.send_item(&"first").map_err(rpc_err)?;
        Err(RpcError { code: -32051, message: "mid-stream".into(), data: None })
    });

    // Concurrency: every in-flight call must reach the barrier before any
    // returns — proves the transport multiplexes concurrent requests rather
    // than serialising them (no wall-clock assertion involved).
    let barrier = Arc::new(tokio::sync::Barrier::new(CONCURRENCY));
    let b = barrier.clone();
    peer.on_request("barrier", move |idx: Idx| {
        let b = b.clone();
        async move {
            b.wait().await;
            Ok::<_, RpcError>(idx.i)
        }
    });

    // Peer symmetry: the server calls back into the client mid-request.
    let p = peer.clone();
    peer.on_request("reverse", move |e: Echo| {
        let p = p.clone();
        async move {
            let r: String = p.call("client_echo", &e).await.map_err(rpc_err)?;
            Ok::<_, RpcError>(r)
        }
    });

    peer.on_request("big", |e: Echo| async move { Ok::<_, RpcError>(e.s) });
}

/// The full client-side battery. `client`'s peer is connected to a server
/// that ran [`register`].
async fn battery(client: &Peer) {
    // The reverse test needs a client-side handler for the server to call.
    client.on_request("client_echo", |e: Echo| async move {
        Ok::<_, RpcError>(format!("echoed:{}", e.s))
    });

    // Unary + typed args.
    let r: String = client.call("echo", &Echo { s: "hi".into() }).await.expect("echo");
    assert_eq!(r, "hi");
    let sum: i64 = client.call("add", &Add { a: 2, b: 40 }).await.expect("add");
    assert_eq!(sum, 42);

    // Error propagation.
    let err = client.call::<_, ()>("fail", &json!({})).await.expect_err("fail");
    match err {
        Error::Rpc(e) => assert_eq!(e.code, -32050),
        other => panic!("expected Rpc error, got {other:?}"),
    }

    // Notification delivery (bounded poll — no timing assertion).
    client.notify("note", &N { n: 3 }).expect("notify");
    let mut delivered = false;
    for _ in 0..200 {
        let c: i64 = client.call("count", &json!({})).await.expect("count");
        if c >= 3 {
            delivered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(delivered, "notification was not delivered");

    // Server-streaming: ordered items then clean end.
    let mut stream: StreamReceiver<u32> =
        client.call_stream("count_stream", &N { n: 5 }).expect("count_stream");
    let mut items = Vec::new();
    while let Some(item) = stream.next().await {
        items.push(item.expect("stream item").data);
    }
    assert_eq!(items, vec![0, 1, 2, 3, 4]);

    // Mid-stream error surfaces after the first item.
    let mut stream: StreamReceiver<String> =
        client.call_stream("boom_stream", &json!({})).expect("boom_stream");
    assert_eq!(stream.next().await.expect("first").expect("ok item").data, "first");
    assert!(stream.next().await.expect("second").is_err(), "expected mid-stream error");

    // Concurrent multiplexing — the barrier deadlocks if serialised, so the
    // timeout is a safety net, not a performance assertion. Args live in a
    // Vec so each in-flight call's borrow outlives the join.
    let idxs: Vec<Idx> = (0..CONCURRENCY as u32).map(|i| Idx { i }).collect();
    let calls = idxs.iter().map(|a| client.call::<_, u32>("barrier", a));
    let mut got = tokio::time::timeout(Duration::from_secs(15), futures::future::try_join_all(calls))
        .await
        .expect("concurrent calls deadlocked (transport not multiplexing?)")
        .expect("barrier calls");
    got.sort_unstable();
    assert_eq!(got, (0..CONCURRENCY as u32).collect::<Vec<_>>());

    // Peer symmetry: server → client call inside a client → server call.
    let rr: String = client.call("reverse", &Echo { s: "x".into() }).await.expect("reverse");
    assert_eq!(rr, "echoed:x");

    // Large frame near the shared ceiling (1 MiB) round-trips intact.
    let big = "a".repeat(1 << 20);
    let back: String = client.call("big", &Echo { s: big.clone() }).await.expect("big");
    assert_eq!(back.len(), big.len());
    assert_eq!(back, big);
}

// ---------------------------------------------------------------------------
// Transport harnesses
// ---------------------------------------------------------------------------

/// A running server that can be dialled repeatedly.
trait Endpoint: Send + Sync {
    fn connect(&self) -> BoxFuture<'_, Result<Connected, String>>;
}

/// A transport under test: stands up a server, hands back an [`Endpoint`].
trait Harness: Send + Sync {
    fn name(&self) -> &'static str;
    fn start(&self, register: PeerHandler) -> BoxFuture<'static, Result<Box<dyn Endpoint>, String>>;
}

static SEQ: AtomicU64 = AtomicU64::new(0);

fn tmp(ext: &str) -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("peerline-conf-{}-{}.{}", std::process::id(), n, ext))
}

async fn dial_retry<F, Fut>(mut dial: F) -> Result<Connected, String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Connected, String>>,
{
    for _ in 0..300 {
        if let Ok(c) = dial().await {
            return Ok(c);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err("dial timed out".into())
}

// --- in-process loopback (baseline: the same battery with no transport) ---
struct Loopback;
impl Harness for Loopback {
    fn name(&self) -> &'static str {
        "loopback"
    }
    fn start(&self, reg: PeerHandler) -> BoxFuture<'static, Result<Box<dyn Endpoint>, String>> {
        Box::pin(async move {
            // Reuse peerline's own in-process pair (as `tests/loopback.rs`
            // does): register the handler set on the server peer, spawn the
            // shared driver, hand back the client peer.
            let (client, server, driver) = peerline::runtime::loopback();
            reg(&server);
            tokio::spawn(driver);
            Ok(Box::new(LoopbackEp { client, _server: server }) as Box<dyn Endpoint>)
        })
    }
}
struct LoopbackEp {
    client: Peer,
    // Held so the in-process pair (and its handlers) stay alive.
    _server: Peer,
}
impl Endpoint for LoopbackEp {
    fn connect(&self) -> BoxFuture<'_, Result<Connected, String>> {
        let client = self.client.clone();
        Box::pin(async move { Ok((client, Box::pin(async {}) as BoxFuture<'static, ()>)) })
    }
}

// --- UDS ---
#[cfg(feature = "uds")]
struct Uds;
#[cfg(feature = "uds")]
impl Harness for Uds {
    fn name(&self) -> &'static str {
        "uds"
    }
    fn start(&self, reg: PeerHandler) -> BoxFuture<'static, Result<Box<dyn Endpoint>, String>> {
        Box::pin(async move {
            let path = tmp("sock");
            let p = path.clone();
            tokio::spawn(async move {
                let _ = peerline_transport_uds::serve(&p, move |peer| reg(peer)).await;
            });
            Ok(Box::new(UdsEp { path }) as Box<dyn Endpoint>)
        })
    }
}
#[cfg(feature = "uds")]
struct UdsEp {
    path: PathBuf,
}
#[cfg(feature = "uds")]
impl Endpoint for UdsEp {
    fn connect(&self) -> BoxFuture<'_, Result<Connected, String>> {
        Box::pin(dial_retry(move || peerline_transport_uds::connect(&self.path)))
    }
}

// --- WS ---
#[cfg(feature = "ws")]
struct Ws;
#[cfg(feature = "ws")]
impl Harness for Ws {
    fn name(&self) -> &'static str {
        "ws"
    }
    fn start(&self, reg: PeerHandler) -> BoxFuture<'static, Result<Box<dyn Endpoint>, String>> {
        Box::pin(async move {
            let addr = free_addr().await?;
            tokio::spawn(async move {
                let _ = peerline_transport_ws::serve(addr, move |peer| reg(peer)).await;
            });
            Ok(Box::new(WsEp { url: format!("ws://{addr}") }) as Box<dyn Endpoint>)
        })
    }
}
#[cfg(feature = "ws")]
struct WsEp {
    url: String,
}
#[cfg(feature = "ws")]
impl Endpoint for WsEp {
    fn connect(&self) -> BoxFuture<'_, Result<Connected, String>> {
        Box::pin(dial_retry(move || peerline_transport_ws::connect(&self.url)))
    }
}

#[cfg(feature = "ws")]
async fn free_addr() -> Result<std::net::SocketAddr, String> {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.map_err(|e| e.to_string())?;
    let addr = probe.local_addr().map_err(|e| e.to_string())?;
    drop(probe);
    Ok(addr)
}

// --- iroh ---
#[cfg(feature = "iroh")]
struct Iroh;
#[cfg(feature = "iroh")]
impl Harness for Iroh {
    fn name(&self) -> &'static str {
        "iroh"
    }
    fn start(&self, reg: PeerHandler) -> BoxFuture<'static, Result<Box<dyn Endpoint>, String>> {
        Box::pin(async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let alpn: Vec<u8> = b"peerline/conformance/1".to_vec();
            let a = alpn.clone();
            let key = peerline_transport_iroh::SecretKey::generate();
            tokio::spawn(async move {
                let _ = peerline_transport_iroh::serve(
                    &a,
                    key,
                    move |t| {
                        let _ = tx.send(t.to_string());
                    },
                    move |peer| reg(peer),
                )
                .await;
            });
            let ticket = tokio::time::timeout(Duration::from_secs(20), rx)
                .await
                .map_err(|_| "iroh ticket timeout".to_string())?
                .map_err(|_| "iroh serve exited before binding".to_string())?;
            Ok(Box::new(IrohEp { ticket, alpn }) as Box<dyn Endpoint>)
        })
    }
}
#[cfg(feature = "iroh")]
struct IrohEp {
    ticket: String,
    alpn: Vec<u8>,
}
#[cfg(feature = "iroh")]
impl Endpoint for IrohEp {
    fn connect(&self) -> BoxFuture<'_, Result<Connected, String>> {
        Box::pin(dial_retry(move || peerline_transport_iroh::connect(&self.ticket, &self.alpn)))
    }
}

// --- Host (a service mounted over WS) ---
#[cfg(feature = "ws")]
struct HostWs;
#[cfg(feature = "ws")]
struct RegService(PeerHandler);
#[cfg(feature = "ws")]
impl peerline_host::Service for RegService {
    fn name(&self) -> &'static str {
        "conf"
    }
    fn register(&self, peer: &Peer) {
        (self.0)(peer)
    }
}
#[cfg(feature = "ws")]
impl Harness for HostWs {
    fn name(&self) -> &'static str {
        "host(ws)"
    }
    fn start(&self, reg: PeerHandler) -> BoxFuture<'static, Result<Box<dyn Endpoint>, String>> {
        Box::pin(async move {
            let addr = free_addr().await?;
            tokio::spawn(async move {
                let host = peerline_host::Host::new()
                    .mount(RegService(reg), peerline_host::Mount::new().ws("/"))
                    .ws_bind(addr)
                    .no_report();
                let _ = host.run().await;
            });
            Ok(Box::new(WsEp { url: format!("ws://{addr}/") }) as Box<dyn Endpoint>)
        })
    }
}

/// All harnesses compiled into this build.
#[allow(clippy::vec_init_then_push)] // pushes are cfg-gated per transport
fn harnesses() -> Vec<Box<dyn Harness>> {
    let mut v: Vec<Box<dyn Harness>> = Vec::new();
    v.push(Box::new(Loopback)); // baseline
    #[cfg(feature = "uds")]
    v.push(Box::new(Uds));
    #[cfg(feature = "ws")]
    v.push(Box::new(Ws));
    #[cfg(feature = "iroh")]
    v.push(Box::new(Iroh));
    #[cfg(feature = "ws")]
    v.push(Box::new(HostWs));
    v
}

async fn run(h: &dyn Harness) {
    let ep = h.start(Arc::new(register)).await.expect("start server");
    let (client, driver) = ep.connect().await.expect("connect");
    tokio::spawn(driver);
    battery(&client).await;
}

#[tokio::test]
async fn conformance_loopback() {
    run(&Loopback).await;
}

#[cfg(feature = "uds")]
#[tokio::test]
async fn conformance_uds() {
    run(&Uds).await;
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn conformance_ws() {
    run(&Ws).await;
}

#[cfg(feature = "iroh")]
#[tokio::test]
async fn conformance_iroh() {
    run(&Iroh).await;
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn conformance_host() {
    run(&HostWs).await;
}

// ---------------------------------------------------------------------------
// Performance comparison (ignored; run explicitly)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "perf comparison; run with --release --ignored --nocapture"]
async fn perf() {
    const SEQ_CALLS: usize = 2_000; // latency
    const CONC_CALLS: usize = 5_000; // throughput
    const BLOB_CALLS: usize = 300; // bandwidth
    let blob = "x".repeat(256 * 1024); // 256 KiB

    println!(
        "\n{:<10} {:>12} {:>14} {:>14}",
        "transport", "latency(µs)", "throughput/s", "bandwidth(MiB/s)"
    );
    println!("{}", "-".repeat(52));

    for h in harnesses() {
        let ep = h.start(Arc::new(register)).await.expect("start");
        let (client, driver) = ep.connect().await.expect("connect");
        tokio::spawn(driver);

        // Warm up.
        let _: String = client.call("echo", &Echo { s: "warm".into() }).await.unwrap();

        // Sequential latency.
        let t = Instant::now();
        for _ in 0..SEQ_CALLS {
            let _: String = client.call("echo", &Echo { s: "p".into() }).await.unwrap();
        }
        let latency_us = t.elapsed().as_secs_f64() * 1e6 / SEQ_CALLS as f64;

        // Concurrent throughput. One shared arg so each in-flight call's
        // borrow outlives the join.
        let arg = Echo { s: "p".into() };
        let t = Instant::now();
        let calls = (0..CONC_CALLS).map(|_| client.call::<_, String>("echo", &arg));
        futures::future::try_join_all(calls).await.unwrap();
        let throughput = CONC_CALLS as f64 / t.elapsed().as_secs_f64();

        // Large-frame bandwidth.
        let t = Instant::now();
        for _ in 0..BLOB_CALLS {
            let _: String = client.call("big", &Echo { s: blob.clone() }).await.unwrap();
        }
        let mib = (BLOB_CALLS * blob.len()) as f64 / (1024.0 * 1024.0);
        let bandwidth = mib / t.elapsed().as_secs_f64();

        println!(
            "{:<10} {:>12.1} {:>14.0} {:>14.1}",
            h.name(),
            latency_us,
            throughput,
            bandwidth
        );
    }
    println!();
}
