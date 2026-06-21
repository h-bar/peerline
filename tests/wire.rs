//! Wire-shape round-trip tests for the peer-symmetric core.

use peerline::peer::{self, InboundKind};
use peerline::wire::{
    self, ERR_INVALID_REQUEST, ERR_PARSE, Frame, Id, Params, Request, Response, ResponseErr,
    RpcError,
};
use serde_json::json;

// ---------------------------------------------------------------------------
// Id — Number | String, no fractional, no null (null lives in Option<Id>)
// ---------------------------------------------------------------------------

#[test]
fn id_deserializes_number_and_string() {
    let n: Id = serde_json::from_str("42").unwrap();
    assert_eq!(n, Id::Number(42));
    let s: Id = serde_json::from_str("\"abc\"").unwrap();
    assert_eq!(s, Id::String("abc".into()));
}

#[test]
fn id_rejects_fractional_number() {
    assert!(serde_json::from_str::<Id>("3.14").is_err());
}

#[test]
fn id_rejects_null_and_bool() {
    assert!(serde_json::from_str::<Id>("null").is_err());
    assert!(serde_json::from_str::<Id>("true").is_err());
}

#[test]
fn id_round_trips() {
    for value in [Id::Number(7), Id::String("k".into())] {
        let s = serde_json::to_string(&value).unwrap();
        let back: Id = serde_json::from_str(&s).unwrap();
        assert_eq!(back, value);
    }
}

#[test]
fn err_response_serializes_none_id_as_null() {
    // Only the Err variant carries Option<Id>; the parse-error case
    // (id: None) must wire as JSON null.
    let r = Response::Err(ResponseErr {
        jsonrpc: Some("2.0".into()),
        id: None,
        error: RpcError {
            code: ERR_PARSE,
            message: "boom".into(),
            data: None,
        },
    });
    let s = serde_json::to_string(&r).unwrap();
    assert!(s.contains("\"id\":null"), "expected id:null, got: {s}");
}

// ---------------------------------------------------------------------------
// Params — Array | Object only
// ---------------------------------------------------------------------------

#[test]
fn params_accepts_array_and_object_only() {
    assert!(matches!(
        serde_json::from_str::<Params>("[1, 2]").unwrap(),
        Params::Array(_)
    ));
    assert!(matches!(
        serde_json::from_str::<Params>(r#"{"x": 1}"#).unwrap(),
        Params::Object(_)
    ));
    assert!(serde_json::from_str::<Params>("42").is_err());
    assert!(serde_json::from_str::<Params>("\"s\"").is_err());
    assert!(serde_json::from_str::<Params>("true").is_err());
    assert!(serde_json::from_str::<Params>("null").is_err());
}

// ---------------------------------------------------------------------------
// Request — id required + non-null
// ---------------------------------------------------------------------------

#[test]
fn request_requires_non_null_id() {
    assert!(serde_json::from_str::<Request>(r#"{"jsonrpc":"2.0","method":"ping"}"#).is_err());
    assert!(
        serde_json::from_str::<Request>(r#"{"jsonrpc":"2.0","method":"ping","id":null}"#).is_err()
    );
}

// ---------------------------------------------------------------------------
// Response — Ok | Err mutual exclusion at the type level
// ---------------------------------------------------------------------------

#[test]
fn response_rejects_both_result_and_error() {
    let frame = r#"{"jsonrpc":"2.0","id":1,"result":"v","error":{"code":-32603,"message":"bad"}}"#;
    assert!(serde_json::from_str::<Response>(frame).is_err());
}

#[test]
fn response_rejects_neither_result_nor_error() {
    assert!(serde_json::from_str::<Response>(r#"{"jsonrpc":"2.0","id":1}"#).is_err());
}

#[test]
fn response_accessors_match_variant() {
    let ok = peer::response_ok_value(Id::Number(1), json!("ok"));
    assert!(ok.is_ok());
    assert_eq!(ok.result(), Some(&json!("ok")));
    assert_eq!(ok.error(), None);
    assert_eq!(ok.id(), Some(&Id::Number(1)));

    let err = peer::response_err(Some(Id::Number(2)), -32000, "boom");
    assert!(err.is_err());
    let e = err.error().expect("error variant");
    assert_eq!(e.code, -32000);
    assert_eq!(e.message, "boom");
}

#[test]
fn response_into_outcome_collapses_to_result() {
    assert_eq!(
        peer::response_ok_value(Id::Number(1), json!(42)).into_outcome(),
        Ok(json!(42))
    );
    match peer::response_err(Some(Id::Number(2)), -32000, "nope").into_outcome() {
        Err(e) => assert_eq!(e.code, -32000),
        Ok(_) => panic!("expected Err"),
    }
}

#[test]
fn response_serializes_without_null_result_and_error() {
    let ok = peer::response_ok_value(Id::Number(1), json!("v"));
    let s = serde_json::to_string(&ok).unwrap();
    assert!(
        !s.contains("\"error\""),
        "ok response shouldn't carry error: {s}"
    );

    let err = peer::response_err(Some(Id::Number(1)), -32603, "bad");
    let s = serde_json::to_string(&err).unwrap();
    assert!(
        !s.contains("\"result\""),
        "err response shouldn't carry result: {s}"
    );
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
// parse_frame — single frame classification
// ---------------------------------------------------------------------------

#[test]
fn parse_frame_classifies_request() {
    let frame = peer::parse_frame(r#"{"jsonrpc":"2.0","method":"ping","id":1}"#).unwrap();
    assert!(matches!(frame, Frame::Request(_)));
}

#[test]
fn parse_frame_classifies_notification() {
    // No id field → Notification
    let frame = peer::parse_frame(r#"{"jsonrpc":"2.0","method":"ping"}"#).unwrap();
    assert!(matches!(frame, Frame::Notification(_)));
}

#[test]
fn parse_frame_treats_null_id_as_notification() {
    // peerline normalizes id:null to no-id (Notification) before
    // deserialize, so requests with an explicit null id classify as
    // notifications rather than failing.
    let frame = peer::parse_frame(r#"{"jsonrpc":"2.0","method":"ping","id":null}"#).unwrap();
    assert!(matches!(frame, Frame::Notification(_)));
}

#[test]
fn parse_frame_classifies_response_ok() {
    let frame = peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"result":"v"}"#).unwrap();
    assert!(matches!(frame, Frame::Response(Response::Ok(_))));
}

#[test]
fn parse_frame_classifies_response_err() {
    let frame =
        peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"x"}}"#).unwrap();
    assert!(matches!(frame, Frame::Response(Response::Err(_))));
}

#[test]
fn parse_frame_returns_parse_error_for_invalid_json() {
    let resp = peer::parse_frame("not json").unwrap_err();
    assert_eq!(resp.error().unwrap().code, ERR_PARSE);
    assert_eq!(resp.id(), None);
}

// ---------------------------------------------------------------------------
// parse_frame — `jsonrpc` version is optional + always accepted
// ---------------------------------------------------------------------------

#[test]
fn parse_frame_accepts_bad_version() {
    // Wrong jsonrpc version still parses successfully — callers who
    // care about spec strictness validate the value themselves.
    let frame = peer::parse_frame(r#"{"jsonrpc":"1.0","method":"a","id":1}"#).unwrap();
    assert!(matches!(frame, Frame::Request(_)));
}

#[test]
fn parse_frame_accepts_missing_version() {
    // Codex app-server and similar peers omit the `jsonrpc` field on
    // the wire entirely. The parser leaves the field as `None`.
    let frame = peer::parse_frame(r#"{"method":"a","id":1}"#).unwrap();
    let Frame::Request(req) = frame else {
        panic!("expected Request, got {frame:?}");
    };
    assert_eq!(req.jsonrpc, None);
}

#[test]
fn validate_version_accepts_2_0_only() {
    assert!(wire::validate_version("2.0").is_ok());
    assert!(wire::validate_version("1.0").is_err());
    assert!(wire::validate_version("").is_err());
}

// ---------------------------------------------------------------------------
// Arrays are rejected at the wire layer — batching is transport's job
// ---------------------------------------------------------------------------

#[test]
fn parse_frame_rejects_array() {
    // Spec §6 batches arrive as JSON arrays. The wire layer doesn't
    // handle them — the transport is expected to split the array into
    // individual frame strings and call parse_frame on each.
    assert!(peer::parse_frame("[]").is_err());
    assert!(peer::parse_frame(r#"[{"jsonrpc":"2.0","method":"a","id":1}]"#).is_err());
}

// ---------------------------------------------------------------------------
// classify — Frame → InboundKind dispatch
// ---------------------------------------------------------------------------

#[test]
fn classify_response_yields_response_kind() {
    let frame = peer::parse_frame(r#"{"jsonrpc":"2.0","id":7,"result":"ok"}"#).unwrap();
    match peer::classify(frame) {
        InboundKind::Response { id, outcome } => {
            assert_eq!(id, Some(Id::Number(7)));
            assert_eq!(outcome.unwrap(), json!("ok"));
        }
        other => panic!("expected Response, got {other:?}"),
    }
}

#[test]
fn classify_request_yields_incoming_request() {
    let frame = peer::parse_frame(r#"{"jsonrpc":"2.0","method":"ping","id":1}"#).unwrap();
    assert!(matches!(
        peer::classify(frame),
        InboundKind::IncomingRequest(_)
    ));
}

#[test]
fn classify_notification_yields_incoming_notification() {
    let frame = peer::parse_frame(r#"{"jsonrpc":"2.0","method":"ping"}"#).unwrap();
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
    // Peer A initiates a request; peer B receives, classifies, and
    // replies; peer A receives the reply.
    let ids = peer::RequestIdGen::new();
    let id = ids.next_id();
    let req = peer::request(id.clone(), "add", &json!([1, 2])).unwrap();
    let req_wire = serde_json::to_string(&Frame::Request(req)).unwrap();

    // peer B
    let inbound = match peer::classify(peer::parse_frame(&req_wire).unwrap()) {
        InboundKind::IncomingRequest(r) => r,
        _ => panic!("expected IncomingRequest"),
    };
    assert_eq!(inbound.method, "add");
    let reply = peer::response_ok_value(inbound.id, json!(3));
    let reply_wire = serde_json::to_string(&Frame::Response(reply)).unwrap();

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
    let n = peer::notification("ping", &json!([])).unwrap();
    let wire = serde_json::to_string(&Frame::Notification(n)).unwrap();
    assert!(
        !wire.contains("\"id\""),
        "notification must not carry id: {wire}"
    );
    match peer::classify(peer::parse_frame(&wire).unwrap()) {
        InboundKind::IncomingNotification(n) => assert_eq!(n.method, "ping"),
        _ => panic!("expected IncomingNotification"),
    }
}

#[test]
fn peer_request_rejects_scalar_params() {
    let err = peer::request(1u64, "foo", &"bare").unwrap_err();
    assert!(err.to_string().contains("Array or Object"));
}

#[test]
fn peer_request_accepts_object_params() {
    let req = peer::request(1u64, "foo", &json!({"a": 1})).unwrap();
    assert_eq!(req.method, "foo");
    assert_eq!(req.id, Id::Number(1));
    assert!(matches!(req.params, Some(Params::Object(_))));
}

// ---------------------------------------------------------------------------
// Frame disambiguation — serde(untagged) order + deny_unknown_fields
//
// Pins down which JSON shape lands in which Frame variant, including
// the malformed-shape cases that must fail at parse time rather than
// silently land in a too-permissive variant.
// ---------------------------------------------------------------------------

#[test]
fn wire_disambiguation_response_ok_shape() {
    let frame: Frame = serde_json::from_str(r#"{"jsonrpc":"2.0","id":1,"result":"v"}"#).unwrap();
    assert!(matches!(frame, Frame::Response(Response::Ok(_))));
}

#[test]
fn wire_disambiguation_response_err_shape() {
    let frame: Frame =
        serde_json::from_str(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"x"}}"#)
            .unwrap();
    assert!(matches!(frame, Frame::Response(Response::Err(_))));
}

#[test]
fn wire_disambiguation_request_shape() {
    // id + method, no result/error → Request
    let frame: Frame = serde_json::from_str(r#"{"jsonrpc":"2.0","id":1,"method":"do"}"#).unwrap();
    assert!(matches!(frame, Frame::Request(_)));
}

#[test]
fn wire_disambiguation_request_with_params() {
    let frame: Frame =
        serde_json::from_str(r#"{"jsonrpc":"2.0","id":1,"method":"do","params":{"x":1}}"#).unwrap();
    assert!(matches!(frame, Frame::Request(_)));
}

#[test]
fn wire_disambiguation_notification_shape() {
    // method, no id → Notification
    let frame: Frame = serde_json::from_str(r#"{"jsonrpc":"2.0","method":"do"}"#).unwrap();
    assert!(matches!(frame, Frame::Notification(_)));
}

#[test]
fn wire_disambiguation_rejects_request_with_result_field() {
    // id + method + result is malformed (looks like both a Request
    // and a Response). deny_unknown_fields on Request rejects
    // `result`; deny_unknown_fields on Response::Ok rejects `method`.
    // Every variant must fail.
    assert!(
        serde_json::from_str::<Frame>(r#"{"jsonrpc":"2.0","id":1,"method":"do","result":"v"}"#)
            .is_err()
    );
}

#[test]
fn wire_disambiguation_rejects_request_with_error_field() {
    assert!(
        serde_json::from_str::<Frame>(
            r#"{"jsonrpc":"2.0","id":1,"method":"do","error":{"code":-1,"message":"x"}}"#
        )
        .is_err()
    );
}

#[test]
fn wire_disambiguation_rejects_notification_with_id() {
    // If the wire actually carries `{method, id}`, the right
    // classification is Request, not Notification. Notification's
    // deny_unknown_fields rejects the id field, forcing the Request
    // variant to win.
    let frame: Frame = serde_json::from_str(r#"{"jsonrpc":"2.0","method":"do","id":5}"#).unwrap();
    assert!(matches!(frame, Frame::Request(_)));
}

#[test]
fn wire_disambiguation_rejects_response_with_method_field() {
    // Reverse direction: a Response-shaped frame with stray method
    // field is malformed.
    assert!(
        serde_json::from_str::<Frame>(
            r#"{"jsonrpc":"2.0","id":1,"result":"v","method":"surprise"}"#
        )
        .is_err()
    );
}

#[test]
fn wire_disambiguation_rejects_id_only_object() {
    // jsonrpc + id but no result/error/method — fits no variant.
    assert!(serde_json::from_str::<Frame>(r#"{"jsonrpc":"2.0","id":1}"#).is_err());
}

#[test]
fn wire_disambiguation_rejects_jsonrpc_only_object() {
    assert!(serde_json::from_str::<Frame>(r#"{"jsonrpc":"2.0"}"#).is_err());
}

#[test]
fn wire_disambiguation_rejects_empty_object() {
    assert!(serde_json::from_str::<Frame>("{}").is_err());
}

#[test]
fn wire_disambiguation_rejects_response_with_both_result_and_error() {
    // Spec §5 mutual exclusion enforced via deny_unknown_fields:
    // ResponseOk rejects the error field, ResponseErr rejects the
    // result field, so both inner variants of Response fail.
    assert!(
        serde_json::from_str::<Frame>(
            r#"{"jsonrpc":"2.0","id":1,"result":"v","error":{"code":-1,"message":"x"}}"#
        )
        .is_err()
    );
}
