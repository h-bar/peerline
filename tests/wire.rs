//! Wire-shape round-trip tests for the peer-symmetric core.

use peerline::peer::{self, InboundKind};
use peerline::wire::{Content, ErrorType, Frame, Response, ResponseErr, RpcError};
use serde_json::json;

// ---------------------------------------------------------------------------
// Frame round-trip — outer ver tag + inner kind tag + short field names
// ---------------------------------------------------------------------------

#[test]
fn request_frame_round_trip() {
    let req = peer::request(7u64, "add", &json!({"a": 1, "b": 2})).unwrap();
    let frame: Frame = req.into();
    let s = serde_json::to_string(&frame).unwrap();
    assert!(s.contains("\"ver\":\"1\""), "expected ver:\"1\": {s}");
    assert!(s.contains("\"kind\":\"req\""), "expected kind:\"req\": {s}");
    assert!(s.contains("\"op\":\"add\""), "expected op:\"add\": {s}");
    assert!(s.contains("\"args\""), "expected args field: {s}");
    let back: Frame = serde_json::from_str(&s).unwrap();
    assert!(matches!(back, Frame::V1(Content::Request(_))));
}

#[test]
fn response_ok_frame_round_trip() {
    let resp = peer::response_ok_value(1u64, json!(42));
    let frame: Frame = resp.into();
    let s = serde_json::to_string(&frame).unwrap();
    assert!(s.contains("\"ver\":\"1\""));
    assert!(s.contains("\"kind\":\"resp\""));
    assert!(s.contains("\"data\":42"));
    assert!(!s.contains("\"err\":"), "ok shouldn't carry err: {s}");
}

#[test]
fn response_err_frame_round_trip() {
    let resp = peer::response_err(Some(1u64), -32000, "boom");
    let frame: Frame = resp.into();
    let s = serde_json::to_string(&frame).unwrap();
    assert!(s.contains("\"ver\":\"1\""));
    assert!(s.contains("\"kind\":\"resp\""));
    assert!(s.contains("\"err\""));
    assert!(!s.contains("\"data\":"), "err shouldn't carry data: {s}");
}

#[test]
fn notification_frame_round_trip() {
    let n = peer::notification("ping", &json!({"x": 1})).unwrap();
    let frame: Frame = n.into();
    let s = serde_json::to_string(&frame).unwrap();
    assert!(s.contains("\"ver\":\"1\""));
    assert!(s.contains("\"kind\":\"notif\""));
    assert!(s.contains("\"op\":\"ping\""));
    assert!(!s.contains("\"id\""), "notification must not carry id: {s}");
}

// ---------------------------------------------------------------------------
// Response shape — Ok | Err mutual exclusion
// ---------------------------------------------------------------------------

#[test]
fn err_response_serializes_none_id_as_null() {
    let r: Response = Response::Err(ResponseErr {
        id: None,
        error: RpcError {
            code: -32700,
            message: "boom".into(),
            data: None,
        },
    });
    let frame: Frame = r.into();
    let s = serde_json::to_string(&frame).unwrap();
    assert!(s.contains("\"id\":null"), "expected id:null, got: {s}");
}

#[test]
fn response_rejects_both_result_and_error() {
    // Response::Ok has deny_unknown_fields (rejects `e`); Response::Err
    // has deny_unknown_fields (rejects `r`); the untagged enum tries
    // both and fails.
    let bad = r#"{"ver":"1","kind":"resp","id":1,"data":"v","err":{"code":-1,"msg":"x"}}"#;
    assert!(serde_json::from_str::<Frame>(bad).is_err());
}

#[test]
fn response_rejects_neither_result_nor_error() {
    let bad = r#"{"ver":"1","kind":"resp","id":1}"#;
    assert!(serde_json::from_str::<Frame>(bad).is_err());
}

#[test]
fn response_accessors_match_variant() {
    let ok = peer::response_ok_value(1u64, json!("ok"));
    assert!(ok.is_ok());
    assert_eq!(ok.result(), Some(&json!("ok")));
    assert_eq!(ok.error(), None);
    assert_eq!(ok.id(), Some(&1u64));

    let err = peer::response_err(Some(2u64), -32000, "boom");
    assert!(err.is_err());
    let e = err.error().expect("error variant");
    assert_eq!(e.code, -32000);
    assert_eq!(e.message, "boom");
}

#[test]
fn response_into_outcome_collapses_to_result() {
    assert_eq!(
        peer::response_ok_value(1u64, json!(42)).into_outcome(),
        Ok(json!(42))
    );
    match peer::response_err(Some(2u64), -32000, "nope").into_outcome() {
        Err(e) => assert_eq!(e.code, -32000),
        Ok(_) => panic!("expected Err"),
    }
}

#[test]
fn rpc_error_data_round_trips() {
    let err = RpcError {
        code: -32000,
        message: "boom".into(),
        data: Some(json!({"sentry_id": "abcd"})),
    };
    let s = serde_json::to_string(&err).unwrap();
    let back: RpcError = serde_json::from_str(&s).unwrap();
    assert_eq!(back.data, err.data);
}

// ---------------------------------------------------------------------------
// parse_frame — single-frame classification
// ---------------------------------------------------------------------------

#[test]
fn parse_frame_classifies_request() {
    let frame = peer::parse_frame(r#"{"ver":"1","kind":"req","op":"ping","id":1}"#).unwrap();
    assert!(matches!(frame, Frame::V1(Content::Request(_))));
}

#[test]
fn parse_frame_classifies_notification() {
    let frame = peer::parse_frame(r#"{"ver":"1","kind":"notif","op":"ping"}"#).unwrap();
    assert!(matches!(frame, Frame::V1(Content::Notification(_))));
}

#[test]
fn parse_frame_classifies_response_ok() {
    let frame = peer::parse_frame(r#"{"ver":"1","kind":"resp","id":1,"data":"v"}"#).unwrap();
    assert!(matches!(
        frame,
        Frame::V1(Content::Response(Response::Ok(_)))
    ));
}

#[test]
fn parse_frame_classifies_response_err() {
    let frame =
        peer::parse_frame(r#"{"ver":"1","kind":"resp","id":1,"err":{"code":-1,"msg":"x"}}"#)
            .unwrap();
    assert!(matches!(
        frame,
        Frame::V1(Content::Response(Response::Err(_)))
    ));
}

#[test]
fn parse_frame_returns_invalid_request_for_invalid_json() {
    let resp = peer::parse_frame("not json").unwrap_err();
    assert_eq!(
        resp.error().unwrap().error_type(),
        ErrorType::InvalidRequest
    );
    assert_eq!(resp.id(), None);
}

#[test]
fn parse_frame_rejects_unknown_version() {
    let resp = peer::parse_frame(r#"{"ver":"999","kind":"req","op":"foo","id":1}"#).unwrap_err();
    assert_eq!(
        resp.error().unwrap().error_type(),
        ErrorType::InvalidRequest
    );
}

#[test]
fn parse_frame_rejects_missing_version() {
    let resp = peer::parse_frame(r#"{"kind":"req","op":"foo","id":1}"#).unwrap_err();
    assert_eq!(
        resp.error().unwrap().error_type(),
        ErrorType::InvalidRequest
    );
}

#[test]
fn parse_frame_rejects_unknown_kind() {
    let resp = peer::parse_frame(r#"{"ver":"1","kind":"unknown","id":1}"#).unwrap_err();
    assert_eq!(
        resp.error().unwrap().error_type(),
        ErrorType::InvalidRequest
    );
}

#[test]
fn parse_frame_rejects_array() {
    // Batching is a transport-layer concern; the wire parser handles
    // one frame at a time.
    assert!(peer::parse_frame("[]").is_err());
    assert!(peer::parse_frame(r#"[{"ver":"1","kind":"req","op":"a","id":1}]"#).is_err());
}

// ---------------------------------------------------------------------------
// classify — Frame → InboundKind dispatch
// ---------------------------------------------------------------------------

#[test]
fn classify_response_yields_response_kind() {
    let frame = peer::parse_frame(r#"{"ver":"1","kind":"resp","id":7,"data":"ok"}"#).unwrap();
    match peer::classify(frame) {
        InboundKind::Response { id, outcome } => {
            assert_eq!(id, Some(7u64));
            assert_eq!(outcome.unwrap(), json!("ok"));
        }
        other => panic!("expected Response, got {other:?}"),
    }
}

#[test]
fn classify_request_yields_incoming_request() {
    let frame = peer::parse_frame(r#"{"ver":"1","kind":"req","op":"ping","id":1}"#).unwrap();
    assert!(matches!(
        peer::classify(frame),
        InboundKind::IncomingRequest(_)
    ));
}

#[test]
fn classify_notification_yields_incoming_notification() {
    let frame = peer::parse_frame(r#"{"ver":"1","kind":"notif","op":"ping"}"#).unwrap();
    assert!(matches!(
        peer::classify(frame),
        InboundKind::IncomingNotification(_)
    ));
}

// ---------------------------------------------------------------------------
// Peer symmetry — same helpers work for either side
// ---------------------------------------------------------------------------

#[test]
fn peer_builders_compose_into_round_trip() {
    let ids = peer::RequestIdGen::new();
    let id = ids.next_id();
    let req = peer::request(id, "add", &json!({"a": 1, "b": 2})).unwrap();
    let req_wire = serde_json::to_string::<Frame>(&req.into()).unwrap();

    // peer B
    let inbound = match peer::classify(peer::parse_frame(&req_wire).unwrap()) {
        InboundKind::IncomingRequest(r) => r,
        _ => panic!("expected IncomingRequest"),
    };
    assert_eq!(inbound.op, "add");
    let reply = peer::response_ok_value(inbound.id, json!(3));
    let reply_wire = serde_json::to_string::<Frame>(&reply.into()).unwrap();

    // peer A
    match peer::classify(peer::parse_frame(&reply_wire).unwrap()) {
        InboundKind::Response { id: rid, outcome } => {
            assert_eq!(rid, Some(id));
            assert_eq!(outcome.unwrap(), json!(3));
        }
        _ => panic!("expected Response"),
    }
}

#[test]
fn peer_notification_round_trip() {
    let n = peer::notification("ping", &json!({})).unwrap();
    let wire = serde_json::to_string::<Frame>(&n.into()).unwrap();
    assert!(
        !wire.contains("\"id\""),
        "notification must not carry id: {wire}"
    );
    match peer::classify(peer::parse_frame(&wire).unwrap()) {
        InboundKind::IncomingNotification(n) => assert_eq!(n.op, "ping"),
        _ => panic!("expected IncomingNotification"),
    }
}

#[test]
fn peer_request_rejects_non_object_params() {
    let err = peer::request(1u64, "foo", &"bare").unwrap_err();
    assert!(err.to_string().contains("Object"));
}

#[test]
fn peer_request_accepts_object_params() {
    let req = peer::request(1u64, "foo", &json!({"a": 1})).unwrap();
    assert_eq!(req.op, "foo");
    assert_eq!(req.id, 1u64);
    assert!(req.args.is_some());
}

// ---------------------------------------------------------------------------
// Smoke tests — tagged-enum disambiguation works
// ---------------------------------------------------------------------------

#[test]
fn wire_disambiguates_via_kind_tag() {
    let req: Frame = serde_json::from_str(r#"{"ver":"1","kind":"req","id":1,"op":"do"}"#).unwrap();
    assert!(matches!(req, Frame::V1(Content::Request(_))));

    let resp: Frame =
        serde_json::from_str(r#"{"ver":"1","kind":"resp","id":1,"data":"v"}"#).unwrap();
    assert!(matches!(resp, Frame::V1(Content::Response(_))));

    let notif: Frame = serde_json::from_str(r#"{"ver":"1","kind":"notif","op":"do"}"#).unwrap();
    assert!(matches!(notif, Frame::V1(Content::Notification(_))));
}

#[test]
fn wire_rejects_request_with_stray_field() {
    // Request's deny_unknown_fields rejects extra fields.
    assert!(
        serde_json::from_str::<Frame>(r#"{"ver":"1","kind":"req","id":1,"op":"do","stray":"v"}"#)
            .is_err()
    );
}
