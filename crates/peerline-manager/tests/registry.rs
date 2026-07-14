//! End-to-end: run the manager on a host over UDS, register a host, see it
//! in list/lookup, then stop heartbeating and watch it expire on TTL.

use std::time::Duration;

use peerline::runtime::Peer;
use peerline_host::{Host, Mount};
use peerline_manager::{Manager, TTL};
use peerline_manager_protocol::{
    Endpoints, Listing, LookupArgs, LookupResult, METHOD_LIST, METHOD_LOOKUP, METHOD_REGISTER,
    RegisterAck, Registration, ServiceEntry,
};
use serde_json::json;

async fn client(sock: &std::path::Path) -> Peer {
    for _ in 0..200 {
        if let Ok((peer, driver)) = peerline_transport_uds::connect(sock).await {
            tokio::spawn(driver);
            return peer;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("manager socket never came up");
}

#[tokio::test]
async fn register_list_lookup_and_expire_on_ttl() {
    let sock = std::env::temp_dir().join(format!("peerline-mgr-{}.sock", std::process::id()));
    let s = sock.clone();
    tokio::spawn(async move {
        let _ = Host::new().mount(Manager::new(), Mount::new().uds(s)).run().await;
    });

    let peer = client(&sock).await;

    // Register one host offering service "foo" over a ws endpoint.
    let reg = Registration {
        host: "h1".into(),
        pid: 4242,
        services: vec![ServiceEntry {
            service: "foo".into(),
            endpoints: Endpoints { ws: vec!["ws://127.0.0.1:9000/".into()], ..Default::default() },
        }],
    };
    let ack: RegisterAck = peer.call(METHOD_REGISTER, &reg).await.expect("register");
    assert_eq!(ack.id, 0);

    // list sees it.
    let listing: Listing = peer.call(METHOD_LIST, &json!({})).await.expect("list");
    assert_eq!(listing.hosts.len(), 1);
    assert_eq!(listing.hosts[0].host, "h1");
    assert_eq!(listing.hosts[0].services[0].service, "foo");

    // lookup resolves the endpoint.
    let found: LookupResult =
        peer.call(METHOD_LOOKUP, &LookupArgs { service: "foo".into() }).await.expect("lookup");
    assert_eq!(found.endpoints.len(), 1);
    assert_eq!(found.endpoints[0].ws, vec!["ws://127.0.0.1:9000/".to_string()]);

    // Without heartbeats the entry expires after TTL and is pruned on list.
    tokio::time::sleep(TTL + Duration::from_millis(500)).await;
    let listing: Listing = peer.call(METHOD_LIST, &json!({})).await.expect("list");
    assert!(listing.hosts.is_empty(), "host was not expired after TTL");
}
