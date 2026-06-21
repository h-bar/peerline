//! Extension tests for the pubsub-subscription module.

use peerline::peer::{self, InboundKind};
use peerline::pubsub::{self, PubsubMessage, SubscriptionIdGen};
use peerline::wire::{Frame, Id, Notification, Params};
use serde_json::json;

#[test]
fn event_helper_returns_notification_with_method_event() {
    let n = pubsub::event("sub-0", &json!({"hello": "world"})).unwrap();
    assert_eq!(n.method, "event");
}

#[test]
fn end_helper_returns_notification_with_method_end() {
    let n = pubsub::end("sub-9");
    assert_eq!(n.method, "end");
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
fn classify_returns_none_for_non_pubsub_method() {
    let n = Notification {
        jsonrpc: Some("2.0".into()),
        method: "log".into(),
        params: Some(Params::Object(serde_json::Map::new())),
    };
    assert!(pubsub::classify(&n).is_none());
}

#[test]
fn classify_returns_none_for_event_with_malformed_params() {
    let n = Notification {
        jsonrpc: Some("2.0".into()),
        method: "event".into(),
        params: Some(Params::Object(serde_json::Map::from_iter([(
            "event".into(),
            json!("x"),
        )]))),
    };
    assert!(pubsub::classify(&n).is_none());
}

#[test]
fn classify_returns_none_for_event_without_params() {
    let n = Notification {
        jsonrpc: Some("2.0".into()),
        method: "event".into(),
        params: None,
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
    let req = pubsub::unsubscribe_request(Id::Number(5), "sub-3");
    assert_eq!(req.method, "unsubscribe");
    assert_eq!(req.id, Id::Number(5));
    let params = req.params.expect("unsubscribe carries params").as_value();
    assert_eq!(params, json!({"subscription_id": "sub-3"}));
}

#[test]
fn unsubscribe_request_accepts_u64_via_into() {
    let req = pubsub::unsubscribe_request(7u64, "sub-x");
    assert_eq!(req.id, Id::Number(7));
}

#[test]
fn subscription_ack_round_trips_inside_response_ok() {
    let ack = pubsub::subscription_ack("sub-42");
    let resp = peer::response_ok(Id::Number(1), &ack).unwrap();
    let result_value = resp.result().expect("ok response carries result");
    let parsed: pubsub::SubscriptionAck = serde_json::from_value(result_value.clone()).unwrap();
    assert_eq!(parsed.subscription_id, "sub-42");
}

// ---------------------------------------------------------------------------
// Composition with peer routing — IncomingNotification → pubsub::classify
// ---------------------------------------------------------------------------

#[test]
fn peer_inbound_notification_pipes_through_pubsub_classify() {
    // End-to-end: pushing peer sends event via pubsub::event(),
    // receiving peer parses the frame, classifies it generically as
    // IncomingNotification, then pipes through pubsub::classify to
    // get the typed PubsubMessage::Event.
    let push = pubsub::event("sub-0", &json!({"k": "v"})).unwrap();
    let wire = serde_json::to_string(&Frame::Notification(push)).unwrap();
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
    let wire = serde_json::to_string(&Frame::Notification(push)).unwrap();
    let notif = match peer::classify(peer::parse_frame(&wire).unwrap()) {
        InboundKind::IncomingNotification(n) => n,
        _ => panic!("expected IncomingNotification"),
    };
    match pubsub::classify(&notif).unwrap() {
        PubsubMessage::End { subscription_id } => assert_eq!(subscription_id, "sub-end"),
        other => panic!("expected PubsubMessage::End, got {other:?}"),
    }
}
