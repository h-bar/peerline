//! `peerline-mon` — a terminal frontend for the peerline manager.
//!
//! Connects to a manager (over its Unix socket by default, or `--ws`), then
//! lists the live registry — once, or continuously with `--watch`.
//!
//! ```text
//! peerline-mon                     # list once (default uds socket)
//! peerline-mon -w                  # live view, refreshed every 2s
//! peerline-mon --sock /run/m.sock  # a specific socket
//! peerline-mon --ws ws://host:6466/
//! peerline-mon lookup <service>    # dial coordinates for one service
//! ```

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::time::Duration;

use peerline::runtime::Peer;
use peerline_manager_protocol::{
    Endpoints, Listing, LookupArgs, LookupResult, METHOD_LIST, METHOD_LOOKUP,
};

enum Target {
    Uds(PathBuf),
    Ws(String),
}

struct Args {
    target: Target,
    watch: bool,
    lookup: Option<String>,
}

fn default_sock() -> PathBuf {
    std::env::var_os("PEERLINE_MANAGER_SOCK")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/peerline-manager.sock"))
}

fn parse_args() -> Result<Args, String> {
    let mut target: Option<Target> = None;
    let mut watch = false;
    let mut lookup = None;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "-w" | "--watch" => watch = true,
            "--sock" => target = Some(Target::Uds(it.next().ok_or("--sock needs a path")?.into())),
            "--ws" => target = Some(Target::Ws(it.next().ok_or("--ws needs a url")?)),
            "lookup" => lookup = Some(it.next().ok_or("lookup needs a service name")?),
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => return Err(format!("unexpected argument: {other}")),
        }
    }
    Ok(Args { target: target.unwrap_or_else(|| Target::Uds(default_sock())), watch, lookup })
}

fn print_help() {
    eprint!(
        "peerline-mon — terminal view of the peerline manager registry.

Usage:
  peerline-mon [--sock PATH | --ws URL] [--watch]
  peerline-mon [--sock PATH | --ws URL] lookup SERVICE

Options:
  --sock PATH   manager Unix socket (default: $PEERLINE_MANAGER_SOCK or
                /tmp/peerline-manager.sock)
  --ws URL      connect over WebSocket instead (ws://host:port/)
  -w, --watch   refresh continuously (every 2s) until Ctrl-C
  -h, --help    this help
"
    );
}

async fn connect(target: &Target) -> Result<Peer, String> {
    let (peer, driver) = match target {
        Target::Uds(path) => peerline_transport_uds::connect(path).await?,
        Target::Ws(url) => peerline_transport_ws::connect(url).await?,
    };
    tokio::spawn(driver);
    Ok(peer)
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args = parse_args()?;
    let peer = connect(&args.target).await.map_err(|e| format!("connect: {e}"))?;

    if let Some(service) = args.lookup {
        let res: LookupResult = peer
            .call(METHOD_LOOKUP, &LookupArgs { service: service.clone() })
            .await
            .map_err(|e| e.to_string())?;
        print_lookup(&service, &res.endpoints);
        return Ok(());
    }

    if args.watch {
        loop {
            let listing = fetch(&peer).await?;
            print!("\x1b[2J\x1b[H"); // clear + home
            println!("peerline manager — {} host(s)   (Ctrl-C to quit)\n", listing.hosts.len());
            print_table(&listing);
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    } else {
        print_table(&fetch(&peer).await?);
        Ok(())
    }
}

async fn fetch(peer: &Peer) -> Result<Listing, String> {
    peer.call(METHOD_LIST, &serde_json::json!({})).await.map_err(|e| e.to_string())
}

fn endpoints_summary(e: &Endpoints) -> String {
    let mut parts = Vec::new();
    parts.extend(e.ws.iter().cloned());
    parts.extend(e.uds.iter().cloned());
    parts.extend(e.iroh.iter().map(|d| format!("iroh:{}", d.alpn)));
    if parts.is_empty() {
        "-".into()
    } else {
        parts.join(", ")
    }
}

fn print_table(listing: &Listing) {
    if listing.hosts.is_empty() {
        println!("(no hosts registered)");
        return;
    }
    // Flatten to rows so columns line up.
    let mut rows: Vec<(String, String, String)> = Vec::new();
    for h in &listing.hosts {
        let host = format!("{} (pid {})", h.host, h.pid);
        if h.services.is_empty() {
            rows.push((host.clone(), "-".into(), "-".into()));
        }
        for (i, s) in h.services.iter().enumerate() {
            rows.push((
                if i == 0 { host.clone() } else { String::new() },
                s.service.clone(),
                endpoints_summary(&s.endpoints),
            ));
        }
    }
    let w0 = rows.iter().map(|r| r.0.len()).max().unwrap_or(4).max(4);
    let w1 = rows.iter().map(|r| r.1.len()).max().unwrap_or(7).max(7);
    println!("{:<w0$}  {:<w1$}  ENDPOINTS", "HOST", "SERVICE", w0 = w0, w1 = w1);
    for (host, service, endpoints) in rows {
        println!("{host:<w0$}  {service:<w1$}  {endpoints}", w0 = w0, w1 = w1);
    }
}

fn print_lookup(service: &str, endpoints: &[Endpoints]) {
    if endpoints.is_empty() {
        println!("no hosts offer service {service:?}");
        return;
    }
    println!("{service}:");
    for e in endpoints {
        for u in &e.ws {
            println!("  ws    {u}");
        }
        for p in &e.uds {
            println!("  uds   {p}");
        }
        for d in &e.iroh {
            println!("  iroh  alpn={} ticket={}", d.alpn, d.ticket);
        }
    }
}
