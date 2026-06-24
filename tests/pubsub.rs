//! Extension tests for the pubsub-subscription module.

use peerline::peer::{self, InboundKind};
use peerline::pubsub::{self, PubsubMessage, SubscriptionIdGen};
use peerline::wire::{Frame, Notification};
use serde_json::{json, Map};

#[test]
fn event_helper_returns_notification_with_op_event() {
    let n = pubsub::event("sub-0", &json!({"hello": "world"})).unwrap();
    assert_eq!(n.op, "event");
}

#[test]
fn end_helper_returns_notification_with_op_end() {
    let n = pubsub::end("sub-9");
    assert_eq!(n.op, "end");
}

#[test]
fn event_round_trips_through_classify() {
    let n = pubsub::event("sub-0", &json!({"hello": "world"})).unwrap();
    match pubsub::classify(&n).unwrap() {
        PubsubMessage::Event {
            subscription_id,
            event,
        } => {
            assert_eq!(subscription_id, "sub-0");
            assert_eq!(event, json!({"hello": "world"}));
        }
        other => panic!("expected Event, got {other:?}"),
    }
}

#[test]
fn end_round_trips_through_classify() {
    let n = pubsub::end("sub-9");
    match pubsub::classify(&n).unwrap() {
        PubsubMessage::End { subscription_id } => assert_eq!(subscription_id, "sub-9"),
        other => panic!("expected End, got {other:?}"),
    }
}

#[test]
fn classify_returns_none_for_non_pubsub_op() {
    let n = Notification {
        op: "log".into(),
        args: Some(Map::new()),
    };
    assert!(pubsub::classify(&n).is_none());
}

#[test]
fn classify_returns_none_for_event_with_malformed_args() {
    let mut args = Map::new();
    args.insert("event".into(), json!("x"));
    let n = Notification {
        op: "event".into(),
        args: Some(args),
    };
    assert!(pubsub::classify(&n).is_none());
}

#[test]
fn classify_returns_none_for_event_without_args() {
    let n = Notification {
        op: "event".into(),
        args: None,
    };
    assert!(pubsub::classify(&n).is_none());
}

#[test]
fn subscription_id_gen_is_monotonic() {
    let g = SubscriptionIdGen::new();
    assert_eq!(g.next(), "sub-0");
    assert_eq!(g.next(), "sub-1");
    assert_eq!(g.next(), "sub-2");
}

#[test]
fn unsubscribe_request_builds_with_typed_id() {
    let req = pubsub::unsubscribe_request(5u64, "sub-3");
    assert_eq!(req.op, "unsubscribe");
    assert_eq!(req.id, 5u64);
    let args = req.args.expect("unsubscribe carries args");
    assert_eq!(serde_json::Value::Object(args), json!({"subscription_id": "sub-3"}));
}

#[test]
fn subscription_ack_round_trips_inside_response_ok() {
    let ack = pubsub::subscription_ack("sub-42");
    let resp = peer::response_ok(1u64, &ack).unwrap();
    let result_value = resp.result().expect("ok response carries result");
    let parsed: pubsub::SubscriptionAck = serde_json::from_value(result_value.clone()).unwrap();
    assert_eq!(parsed.subscription_id, "sub-42");
}

// ---------------------------------------------------------------------------
// Composition with peer routing — IncomingNotification → pubsub::classify
// ---------------------------------------------------------------------------

#[test]
fn peer_inbound_notification_pipes_through_pubsub_classify() {
    let push = pubsub::event("sub-0", &json!({"k": "v"})).unwrap();
    let wire = serde_json::to_string::<Frame>(&push.into()).unwrap();
    let frame = peer::parse_frame(&wire).unwrap();
    let notif = match peer::classify(frame) {
        InboundKind::IncomingNotification(n) => n,
        other => panic!("expected IncomingNotification, got {other:?}"),
    };
    match pubsub::classify(&notif).unwrap() {
        PubsubMessage::Event {
            subscription_id,
            event,
        } => {
            assert_eq!(subscription_id, "sub-0");
            assert_eq!(event, json!({"k": "v"}));
        }
        other => panic!("expected PubsubMessage::Event, got {other:?}"),
    }
}

#[test]
fn peer_inbound_end_notification_pipes_through_pubsub_classify() {
    let push = pubsub::end("sub-end");
    let wire = serde_json::to_string::<Frame>(&push.into()).unwrap();
    let notif = match peer::classify(peer::parse_frame(&wire).unwrap()) {
        InboundKind::IncomingNotification(n) => n,
        _ => panic!("expected IncomingNotification"),
    };
    match pubsub::classify(&notif).unwrap() {
        PubsubMessage::End { subscription_id } => assert_eq!(subscription_id, "sub-end"),
        other => panic!("expected PubsubMessage::End, got {other:?}"),
    }
}
