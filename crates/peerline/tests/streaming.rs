//! Streaming-layer tests.
//!
//! `StreamFrame` is a flat struct with lifecycle encoded in `seq`:
//! `seq >= 0` = item (0-indexed), `seq == -1` = terminal. `data` and
//! `error` are optional on any frame. Receivers treat any `error`-bearing frame
//! or any `seq == -1` frame as terminal.

use peerline::peer::{self, InboundKind};
use peerline::wire::{Content, Frame, STREAM_TERMINAL_SEQ};
use serde_json::json;

// ---------------------------------------------------------------------------
// Builders + round-trip
// ---------------------------------------------------------------------------

#[test]
fn stream_item_round_trips() {
    let frame = peer::stream_item(7u64, 1, &json!({"chunk": 1})).unwrap();
    assert_eq!(frame.id, 7);
    assert_eq!(frame.seq, 1);
    assert_eq!(frame.data.as_ref().unwrap(), &json!({"chunk": 1}));
    assert!(frame.error.is_none());
    assert!(!frame.is_terminal());

    let wire = serde_json::to_string::<Frame>(&frame.into()).unwrap();
    assert!(wire.contains("\"seq\":1"));
    assert!(wire.contains("\"data\""));
    assert!(
        !wire.contains("\"ph\""),
        "ph field should not exist anymore: {wire}"
    );

    let back = match serde_json::from_str::<Frame>(&wire).unwrap() {
        Frame::V1(Content::Stream(s)) => s,
        other => panic!("expected Stream, got {other:?}"),
    };
    assert_eq!(back.seq, 1);
    assert_eq!(back.data.as_ref().unwrap(), &json!({"chunk": 1}));
}

#[test]
fn stream_terminal_empty_round_trips() {
    let frame = peer::stream_terminal(7u64);
    assert_eq!(frame.id, 7);
    assert_eq!(frame.seq, STREAM_TERMINAL_SEQ);
    assert!(frame.data.is_none());
    assert!(frame.error.is_none());
    assert!(frame.is_terminal());

    let wire = serde_json::to_string::<Frame>(&frame.into()).unwrap();
    assert!(wire.contains("\"seq\":-1"));
    assert!(
        !wire.contains("\"data\""),
        "empty terminal shouldn't carry data: {wire}"
    );
    assert!(
        !wire.contains("\"err\""),
        "empty terminal shouldn't carry err: {wire}"
    );
}

#[test]
fn stream_terminal_with_data_round_trips() {
    let frame = peer::stream_terminal_with_data(7u64, &json!({"final": true})).unwrap();
    assert_eq!(frame.seq, STREAM_TERMINAL_SEQ);
    assert_eq!(frame.data.as_ref().unwrap(), &json!({"final": true}));
    assert!(frame.error.is_none());
    assert!(frame.is_terminal());

    let wire = serde_json::to_string::<Frame>(&frame.into()).unwrap();
    assert!(wire.contains("\"seq\":-1"));
    assert!(wire.contains("\"data\""));
}

#[test]
fn stream_terminal_with_error_round_trips() {
    let frame = peer::stream_terminal_with_error(7u64, -32000, "boom");
    assert_eq!(frame.seq, STREAM_TERMINAL_SEQ);
    assert!(frame.data.is_none());
    let e = frame.error.as_ref().expect("error present");
    assert_eq!(e.code, -32000);
    assert_eq!(e.message, "boom");
    assert!(frame.is_terminal());

    let wire = serde_json::to_string::<Frame>(&frame.into()).unwrap();
    assert!(wire.contains("\"seq\":-1"));
    assert!(wire.contains("\"err\""));
}

#[test]
fn stream_terminal_with_error_data_carries_inner_data() {
    let frame =
        peer::stream_terminal_with_error_data(7u64, -32000, "boom", json!({"sentry": "abc"}));
    assert!(frame.is_terminal());
    let e = frame.error.as_ref().unwrap();
    assert_eq!(e.data, Some(json!({"sentry": "abc"})));
}

#[test]
fn stream_terminal_with_rpc_error_preserves_full_error() {
    let frame = peer::stream_terminal_with_rpc_error(
        7u64,
        peerline::wire::RpcError {
            code: -32001,
            message: "boom".into(),
            data: Some(json!([1, 2])),
        },
    );
    assert!(frame.is_terminal());
    assert_eq!(frame.seq, STREAM_TERMINAL_SEQ);
    assert!(frame.data.is_none());

    // Round-trip: the error's data survives the wire.
    let s = serde_json::to_string(&Frame::from(frame)).unwrap();
    let back: Frame = serde_json::from_str(&s).unwrap();
    let Frame::V1(Content::Stream(sf)) = back else {
        panic!("expected stream frame");
    };
    let e = sf.error.expect("error present");
    assert_eq!(e.code, -32001);
    assert_eq!(e.data, Some(json!([1, 2])));
}

// ---------------------------------------------------------------------------
// Wire parsing — happy path
// ---------------------------------------------------------------------------

#[test]
fn parse_frame_classifies_stream_item() {
    let frame =
        peer::parse_frame(r#"{"ver":"1","kind":"stream","id":1,"seq":42,"data":[1,2,3]}"#).unwrap();
    let s = match frame {
        Frame::V1(Content::Stream(s)) => s,
        other => panic!("expected Stream, got {other:?}"),
    };
    assert_eq!(s.id, 1);
    assert_eq!(s.seq, 42);
    assert_eq!(s.data.as_ref().unwrap(), &json!([1, 2, 3]));
    assert!(s.error.is_none());
}

#[test]
fn parse_frame_classifies_empty_terminal() {
    let frame = peer::parse_frame(r#"{"ver":"1","kind":"stream","id":1,"seq":-1}"#).unwrap();
    let s = match frame {
        Frame::V1(Content::Stream(s)) => s,
        other => panic!("expected Stream, got {other:?}"),
    };
    assert!(s.is_terminal());
    assert!(s.data.is_none());
    assert!(s.error.is_none());
}

#[test]
fn parse_frame_classifies_terminal_with_data() {
    let frame =
        peer::parse_frame(r#"{"ver":"1","kind":"stream","id":1,"seq":-1,"data":{"final":true}}"#)
            .unwrap();
    let s = match frame {
        Frame::V1(Content::Stream(s)) => s,
        other => panic!("expected Stream, got {other:?}"),
    };
    assert!(s.is_terminal());
    assert_eq!(s.data.as_ref().unwrap(), &json!({"final": true}));
}

#[test]
fn parse_frame_classifies_terminal_with_error() {
    let frame = peer::parse_frame(
        r#"{"ver":"1","kind":"stream","id":1,"seq":-1,"err":{"code":-32000,"msg":"boom"}}"#,
    )
    .unwrap();
    let s = match frame {
        Frame::V1(Content::Stream(s)) => s,
        other => panic!("expected Stream, got {other:?}"),
    };
    assert!(s.is_terminal());
    assert_eq!(s.error.as_ref().unwrap().code, -32000);
}

// ---------------------------------------------------------------------------
// Wire-level rejection of malformed frames
// ---------------------------------------------------------------------------

#[test]
fn stream_frame_rejects_unknown_field() {
    // deny_unknown_fields blocks stray fields.
    let frame =
        peer::parse_frame(r#"{"ver":"1","kind":"stream","id":1,"seq":1,"data":1,"surprise":42}"#);
    assert!(frame.is_err(), "stray field should be rejected");
}

#[test]
fn stream_frame_rejects_missing_seq() {
    let frame = peer::parse_frame(r#"{"ver":"1","kind":"stream","id":1,"data":42}"#);
    assert!(frame.is_err(), "seq is required");
}

// ---------------------------------------------------------------------------
// classify — InboundKind::Stream
// ---------------------------------------------------------------------------

#[test]
fn classify_stream_yields_stream_kind() {
    let frame = peer::parse_frame(r#"{"ver":"1","kind":"stream","id":1,"seq":3,"data":{"k":"v"}}"#)
        .unwrap();
    match peer::classify(frame) {
        InboundKind::Stream(s) => {
            assert_eq!(s.id, 1);
            assert_eq!(s.seq, 3);
            assert_eq!(s.data.as_ref().unwrap(), &json!({"k": "v"}));
        }
        other => panic!("expected Stream, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// End-to-end lifecycle — items → terminal
// ---------------------------------------------------------------------------

#[test]
fn server_stream_lifecycle_items_then_terminal() {
    let id = 42u64;
    let pushed = vec![
        peer::stream_item(id, 1, &json!(1)).unwrap(),
        peer::stream_item(id, 2, &json!(2)).unwrap(),
        peer::stream_terminal(id),
    ];

    let mut classifications = Vec::new();
    let mut items: Vec<serde_json::Value> = Vec::new();
    for sf in pushed {
        let wire = serde_json::to_string::<Frame>(&sf.into()).unwrap();
        match peer::classify(peer::parse_frame(&wire).unwrap()) {
            InboundKind::Stream(s) => {
                assert_eq!(s.id, id);
                if s.is_terminal() {
                    classifications.push("terminal");
                } else {
                    classifications.push("item");
                }
                if let Some(d) = s.data {
                    items.push(d.to_value().unwrap());
                }
            }
            other => panic!("expected Stream, got {other:?}"),
        }
    }

    assert_eq!(classifications, vec!["item", "item", "terminal"]);
    assert_eq!(items, vec![json!(1), json!(2)]);
}

#[test]
fn terminal_with_bundled_last_item() {
    let id = 99u64;
    let frame = peer::stream_terminal_with_data(id, &json!("last")).unwrap();
    let wire = serde_json::to_string::<Frame>(&frame.into()).unwrap();
    match peer::classify(peer::parse_frame(&wire).unwrap()) {
        InboundKind::Stream(s) => {
            assert!(s.is_terminal());
            assert_eq!(s.data.as_ref().unwrap(), &json!("last"));
        }
        other => panic!("expected Stream, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Disambiguation: Stream variant doesn't collide with Response / Request
// ---------------------------------------------------------------------------

#[test]
fn stream_does_not_match_response_or_request_shapes() {
    let resp_frame = peer::parse_frame(r#"{"ver":"1","kind":"resp","id":1,"data":"v"}"#).unwrap();
    assert!(matches!(resp_frame, Frame::V1(Content::Response(_))));

    let req_frame = peer::parse_frame(r#"{"ver":"1","kind":"req","id":1,"op":"x"}"#).unwrap();
    assert!(matches!(req_frame, Frame::V1(Content::Request(_))));
}
