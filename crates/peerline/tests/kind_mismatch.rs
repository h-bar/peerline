//! Request-kind mismatch between the two ends.
//!
//! The wire carries no "I want a stream" marker — whether a request is
//! answered with one `resp` frame or a run of `stream` frames is decided
//! entirely by which handler the *responder* registered. So the two ends
//! can disagree, and a frame can arrive whose kind doesn't match the
//! registry the caller filed its id under.
//!
//! Every case here used to leave the caller waiting until the connection
//! closed. The contract now is that the caller always gets an error, and
//! that a mistyped op still surfaces as `MethodNotFound` rather than as
//! something this layer synthesized.

#![cfg(feature = "runtime")]

use std::time::Duration;

use futures::stream::StreamExt;
use peerline::runtime::{Error, ProtocolError, loopback};
use peerline::wire::{ErrorType, RpcError};
use serde_json::json;

/// Every await here is expected to complete promptly; the timeout is a
/// hang guard, not a timing assertion.
const GUARD: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// call_stream against a unary responder
// ---------------------------------------------------------------------------

/// `call_stream` on an op nobody handles: the remote's `MethodNotFound`
/// arrives as a `resp` frame for an id filed under the *stream*
/// registry. It must reach the receiver as an error terminal, carrying
/// the remote's own error — a typo'd op is the most likely way to hit
/// this and `MethodNotFound` is what makes it diagnosable.
#[tokio::test]
async fn call_stream_on_unknown_op_yields_method_not_found() {
    let (client, _server, driver) = loopback();
    tokio::spawn(driver);

    let mut rx = client
        .call_stream::<_, serde_json::Value>("nope", &json!({}))
        .expect("call_stream");

    let item = tokio::time::timeout(GUARD, rx.next())
        .await
        .expect("receiver must not hang")
        .expect("a terminal must be delivered");

    match item {
        Err(Error::Rpc(e)) => {
            assert_eq!(e.error_type(), ErrorType::MethodNotFound);
            assert!(e.message.contains("nope"), "unexpected message: {e:?}");
        }
        other => panic!("expected MethodNotFound, got {other:?}"),
    }

    // The error terminal ends the stream.
    assert!(
        tokio::time::timeout(GUARD, rx.next())
            .await
            .expect("receiver must not hang")
            .is_none()
    );
}

/// `call_stream` on an op the remote handles *unarily*. The op exists,
/// so there is no remote error to forward — the mismatch itself is the
/// error, and it must not be coerced into a one-item stream (a shape the
/// wire never describes, and one the TypeScript implementation would
/// then have to mirror exactly).
#[tokio::test]
async fn call_stream_on_unary_handler_yields_protocol_error() {
    let (client, server, driver) = loopback();
    tokio::spawn(driver);
    server.on_request("unary", |_: serde_json::Value| async {
        Ok::<_, RpcError>(42)
    });

    let mut rx = client
        .call_stream::<_, serde_json::Value>("unary", &json!({}))
        .expect("call_stream");

    match tokio::time::timeout(GUARD, rx.next())
        .await
        .expect("receiver must not hang")
        .expect("a terminal must be delivered")
    {
        Err(Error::Rpc(e)) => {
            assert_eq!(e.error_type(), ErrorType::Internal);
            assert!(e.message.contains("unary"), "unexpected message: {e:?}");
        }
        other => panic!("expected a protocol error, got {other:?}"),
    }
}

/// A unary handler that *fails* still forwards its own error verbatim,
/// `data` payload included — the mismatch must not mask what the remote
/// actually said.
#[tokio::test]
async fn call_stream_on_failing_unary_handler_forwards_the_error() {
    let (client, server, driver) = loopback();
    tokio::spawn(driver);
    server.on_request("boom", |_: serde_json::Value| async {
        Err::<serde_json::Value, _>(RpcError {
            code: 4041,
            message: "no such widget".into(),
            data: Some(json!({"widget": 7})),
        })
    });

    let mut rx = client
        .call_stream::<_, serde_json::Value>("boom", &json!({}))
        .expect("call_stream");

    match tokio::time::timeout(GUARD, rx.next())
        .await
        .expect("receiver must not hang")
        .expect("a terminal must be delivered")
    {
        Err(Error::Rpc(e)) => {
            assert_eq!(e.code, 4041);
            assert_eq!(e.message, "no such widget");
            assert_eq!(e.data, Some(json!({"widget": 7})));
        }
        other => panic!("expected the handler's error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// call against a streaming responder
// ---------------------------------------------------------------------------

/// The mirror case: a unary `call` on an op the remote streams. The
/// waiter can only take one value, so the first stream frame fails it.
#[tokio::test]
async fn call_on_stream_handler_fails_the_waiter() {
    let (client, server, driver) = loopback();
    tokio::spawn(driver);
    server.on_stream_request("streamy", |_: serde_json::Value, sender| async move {
        sender.send_item(&1).ok();
        sender.send_item(&2).ok();
        Ok::<_, RpcError>(())
    });

    let got = tokio::time::timeout(
        GUARD,
        client.call::<_, serde_json::Value>("streamy", &json!({})),
    )
    .await
    .expect("call must not hang");

    match got {
        Err(Error::Rpc(e)) => {
            assert_eq!(e.error_type(), ErrorType::Internal);
            assert!(
                e.message.contains("call_stream"),
                "unexpected message: {e:?}"
            );
        }
        other => panic!("expected a protocol error, got {:?}", other.map(|_| ())),
    }
}

/// When the stream handler fails, its error is the informative one and
/// is forwarded to the unary waiter verbatim.
#[tokio::test]
async fn call_on_failing_stream_handler_forwards_the_error() {
    let (client, server, driver) = loopback();
    tokio::spawn(driver);
    server.on_stream_request("streamy", |_: serde_json::Value, _sender| async move {
        Err::<(), _>(RpcError {
            code: 4042,
            message: "upstream gone".into(),
            data: None,
        })
    });

    match tokio::time::timeout(
        GUARD,
        client.call::<_, serde_json::Value>("streamy", &json!({})),
    )
    .await
    .expect("call must not hang")
    {
        Err(Error::Rpc(e)) => {
            assert_eq!(e.code, 4042);
            assert_eq!(e.message, "upstream gone");
        }
        other => panic!("expected the handler's error, got {:?}", other.map(|_| ())),
    }
}

// ---------------------------------------------------------------------------
// Protocol-error hook
// ---------------------------------------------------------------------------

/// Frames that stay unroutable after the fallbacks above — here the
/// trailing frames of a stream whose unary waiter has already been
/// failed — are still discarded, but the hook sees them.
#[tokio::test]
async fn unroutable_frames_reach_the_protocol_error_hook() {
    use futures::channel::mpsc;

    let (client, server, driver) = loopback();
    tokio::spawn(driver);

    // A channel, not a counter polled in a spin loop: awaiting the
    // report is deterministic, where spinning would be a bet on how far
    // the dispatch loop gets in N yields.
    let (seen_tx, mut seen_rx) = mpsc::unbounded::<ProtocolError>();
    client.on_protocol_error(move |e| {
        if matches!(e, ProtocolError::UnroutableFrame { .. }) {
            let _ = seen_tx.unbounded_send(e);
        }
    });

    server.on_stream_request("streamy", |_: serde_json::Value, sender| async move {
        for i in 0..3 {
            sender.send_item(&i).ok();
        }
        Ok::<_, RpcError>(())
    });

    // The first stream frame fails the waiter; the rest are unroutable.
    let _ = tokio::time::timeout(
        GUARD,
        client.call::<_, serde_json::Value>("streamy", &json!({})),
    )
    .await
    .expect("call must not hang");

    assert!(
        tokio::time::timeout(GUARD, seen_rx.next())
            .await
            .expect("a trailing frame must be reported")
            .is_some(),
        "trailing stream frames should have been reported as unroutable"
    );
}

/// A malformed inbound frame is answered with an `id: null` error and
/// reported locally — previously it was invisible from inside the
/// process.
#[tokio::test]
async fn malformed_frames_reach_the_protocol_error_hook() {
    use futures::channel::mpsc;
    use futures::sink::SinkExt;
    use peerline::runtime::Peer;
    use std::convert::Infallible;

    let (mut in_tx, in_rx) = mpsc::unbounded::<Result<String, Infallible>>();
    let (out_tx, _out_rx) = mpsc::unbounded::<String>();
    let (peer, driver) = Peer::new(out_tx, in_rx);

    let (seen_tx, mut seen_rx) = mpsc::unbounded::<ProtocolError>();
    peer.on_protocol_error(move |e| {
        let _ = seen_tx.unbounded_send(e);
    });
    tokio::spawn(driver);

    in_tx.send(Ok("{not json".to_string())).await.expect("send");

    let reported = tokio::time::timeout(GUARD, seen_rx.next())
        .await
        .expect("hook must fire")
        .expect("hook must fire");
    assert!(
        matches!(reported, ProtocolError::MalformedFrame { .. }),
        "unexpected report: {reported:?}"
    );
}

/// A response the remote could not correlate (`id: null`) can't be
/// matched here either. It stays discarded — but it is now observable
/// instead of being an unexplained pending call.
#[tokio::test]
async fn uncorrelated_response_reaches_the_protocol_error_hook() {
    use futures::channel::mpsc;
    use futures::sink::SinkExt;
    use peerline::runtime::Peer;
    use std::convert::Infallible;

    let (mut in_tx, in_rx) = mpsc::unbounded::<Result<String, Infallible>>();
    let (out_tx, _out_rx) = mpsc::unbounded::<String>();
    let (peer, driver) = Peer::new(out_tx, in_rx);

    let (seen_tx, mut seen_rx) = mpsc::unbounded::<ProtocolError>();
    peer.on_protocol_error(move |e| {
        let _ = seen_tx.unbounded_send(e);
    });
    tokio::spawn(driver);

    in_tx
        .send(Ok(
            r#"{"ver":"1","kind":"resp","id":null,"err":{"code":-32600,"msg":"bad"}}"#.to_string(),
        ))
        .await
        .expect("send");

    match tokio::time::timeout(GUARD, seen_rx.next())
        .await
        .expect("hook must fire")
        .expect("hook must fire")
    {
        ProtocolError::UncorrelatedResponse { error } => {
            let error = error.expect("the reported error should carry the remote's error");
            assert_eq!(error.error_type(), ErrorType::InvalidRequest);
        }
        other => panic!("unexpected report: {other:?}"),
    }
}

/// A unary reply is final, so a *duplicate* of it must not push a second
/// error past the terminal — `StreamReceiver` promises at most one final
/// `Err` before completing. The duplicate is unroutable instead.
#[tokio::test]
async fn duplicate_unary_reply_does_not_yield_a_second_error() {
    use futures::channel::mpsc;
    use futures::sink::SinkExt;
    use peerline::runtime::Peer;
    use std::convert::Infallible;

    let (mut in_tx, in_rx) = mpsc::unbounded::<Result<String, Infallible>>();
    let (out_tx, mut out_rx) = mpsc::unbounded::<String>();
    let (peer, driver) = Peer::new(out_tx, in_rx);

    let (seen_tx, mut seen_rx) = mpsc::unbounded::<ProtocolError>();
    peer.on_protocol_error(move |e| {
        let _ = seen_tx.unbounded_send(e);
    });
    tokio::spawn(driver);

    let mut rx = peer
        .call_stream::<_, serde_json::Value>("unary", &json!({}))
        .expect("call_stream");
    // Learn the id the peer allocated by reading the request it wrote.
    let sent = tokio::time::timeout(GUARD, out_rx.next())
        .await
        .expect("request must be written")
        .expect("request must be written");
    let id = serde_json::from_str::<serde_json::Value>(&sent).expect("json")["id"]
        .as_u64()
        .expect("id");

    let reply = format!(r#"{{"ver":"1","kind":"resp","id":{id},"data":1}}"#);
    in_tx.send(Ok(reply.clone())).await.expect("send");
    in_tx.send(Ok(reply)).await.expect("send");

    // Exactly one error, then end of stream.
    assert!(matches!(
        tokio::time::timeout(GUARD, rx.next())
            .await
            .expect("no hang"),
        Some(Err(Error::Rpc(_)))
    ));
    assert!(
        tokio::time::timeout(GUARD, rx.next())
            .await
            .expect("no hang")
            .is_none(),
        "the duplicate must not surface as a second item"
    );

    match tokio::time::timeout(GUARD, seen_rx.next())
        .await
        .expect("hook must fire")
        .expect("hook must fire")
    {
        ProtocolError::UnroutableFrame { id: reported } => assert_eq!(reported, id),
        other => panic!("unexpected report: {other:?}"),
    }
}
