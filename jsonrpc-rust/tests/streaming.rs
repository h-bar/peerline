//! Streaming-extension tests (proposed JSON-RPC 2.1 — see
//! `workbench/jsonrpc-rust-streaming.md`).
//!
//! `StreamFrame` is a phase-per-variant enum so the type system
//! enforces the `data` ⇔ `Item` and `error` ⇔ `Error` invariants.
//! There's no malformed `StreamFrame` value you can construct in
//! Rust — these tests therefore focus on wire-level round-trip and
//! deserialization edge cases (e.g. stray fields rejected by
//! `deny_unknown_fields`).

use jsonrpc_rust::peer::{self, InboundKind};
use jsonrpc_rust::wire::{self, Frame, Id, StreamFrame};
use serde_json::json;

// ---------------------------------------------------------------------------
// Per-variant builders + round-trip
// ---------------------------------------------------------------------------

#[test]
fn stream_open_round_trips() {
    let frame = peer::stream_open(7u64);
    assert!(matches!(frame, StreamFrame::Open { .. }));
    assert_eq!(frame.id(), &Id::Number(7));

    let wire = serde_json::to_string(&Frame::Stream(frame)).unwrap();
    assert!(wire.contains("\"stream\":\"open\""));
    let back = match serde_json::from_str::<Frame>(&wire).unwrap() {
        Frame::Stream(s) => s,
        other => panic!("expected Frame::Stream, got {other:?}"),
    };
    assert!(matches!(back, StreamFrame::Open { .. }));
}

#[test]
fn stream_item_round_trips() {
    let frame = peer::stream_item(7u64, 1, &json!({"chunk": 1})).unwrap();
    match &frame {
        StreamFrame::Item { id, seq, data, .. } => {
            assert_eq!(id, &Id::Number(7));
            assert_eq!(*seq, 1);
            assert_eq!(data, &json!({"chunk": 1}));
        }
        other => panic!("expected Item, got {other:?}"),
    }
    let wire = serde_json::to_string(&Frame::Stream(frame)).unwrap();
    assert!(wire.contains("\"stream\":\"item\""));
    assert!(wire.contains("\"seq\":1"));
    assert!(wire.contains("\"data\""));
    let back = match serde_json::from_str::<Frame>(&wire).unwrap() {
        Frame::Stream(s) => s,
        other => panic!("expected Frame::Stream, got {other:?}"),
    };
    match back {
        StreamFrame::Item { seq, data, .. } => {
            assert_eq!(seq, 1);
            assert_eq!(data, json!({"chunk": 1}));
        }
        other => panic!("expected Item, got {other:?}"),
    }
}

#[test]
fn stream_close_round_trips() {
    let frame = peer::stream_close(7u64);
    assert!(matches!(frame, StreamFrame::Close { .. }));
    let wire = serde_json::to_string(&Frame::Stream(frame)).unwrap();
    assert!(wire.contains("\"stream\":\"close\""));
    assert!(!wire.contains("\"data\""));
    assert!(!wire.contains("\"error\""));
}

#[test]
fn stream_error_round_trips() {
    let frame = peer::stream_error(7u64, -32000, "boom");
    match &frame {
        StreamFrame::Error { error, .. } => {
            assert_eq!(error.code, -32000);
            assert_eq!(error.message, "boom");
        }
        other => panic!("expected Error, got {other:?}"),
    }
    let wire = serde_json::to_string(&Frame::Stream(frame)).unwrap();
    assert!(wire.contains("\"stream\":\"error\""));
    assert!(wire.contains("\"error\""));
}

#[test]
fn stream_error_with_data_carries_data_on_inner_rpc_error() {
    let frame = peer::stream_error_with_data(7u64, -32000, "boom", json!({"sentry": "abc"}));
    match frame {
        StreamFrame::Error { error, .. } => {
            assert_eq!(error.data, Some(json!({"sentry": "abc"})));
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[test]
fn stream_cancel_round_trips() {
    let frame = peer::stream_cancel(7u64);
    assert!(matches!(frame, StreamFrame::Cancel { .. }));
    let wire = serde_json::to_string(&Frame::Stream(frame)).unwrap();
    assert!(wire.contains("\"stream\":\"cancel\""));
    assert!(!wire.contains("\"data\""));
    assert!(!wire.contains("\"error\""));
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

#[test]
fn id_accessor_works_on_every_variant() {
    let id = Id::Number(42);
    assert_eq!(peer::stream_open(id.clone()).id(), &id);
    assert_eq!(
        peer::stream_item(id.clone(), 1, &json!(0)).unwrap().id(),
        &id
    );
    assert_eq!(peer::stream_close(id.clone()).id(), &id);
    assert_eq!(peer::stream_error(id.clone(), -1, "x").id(), &id);
    assert_eq!(peer::stream_cancel(id.clone()).id(), &id);
}

#[test]
fn jsonrpc_accessor_works_on_every_variant() {
    for frame in [
        peer::stream_open(1u64),
        peer::stream_item(1u64, 1, &json!(0)).unwrap(),
        peer::stream_close(1u64),
        peer::stream_error(1u64, -1, "x"),
        peer::stream_cancel(1u64),
    ] {
        assert_eq!(frame.jsonrpc(), Some("2.0"));
    }
}

// ---------------------------------------------------------------------------
// Wire-level rejection of invalid combos (the type system can't
// rebuild an invalid frame in Rust; these test the deserialize side)
// ---------------------------------------------------------------------------

#[test]
fn stream_open_with_stray_data_field_rejected() {
    // Open variant has only jsonrpc + id; data is an unknown field.
    let frame = peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"stream":"open","data":42}"#);
    assert!(frame.is_err(), "Open must reject unknown `data` field");
}

#[test]
fn stream_item_without_data_rejected() {
    // Item variant requires data; missing field fails.
    let frame = peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"stream":"item","seq":1}"#);
    assert!(frame.is_err(), "Item must require `data`");
}

#[test]
fn stream_item_without_seq_rejected() {
    // Item variant requires seq; missing field fails.
    let frame = peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"stream":"item","data":1}"#);
    assert!(frame.is_err(), "Item must require `seq`");
}

#[test]
fn stream_item_with_extra_error_field_rejected() {
    let frame = peer::parse_frame(
        r#"{"jsonrpc":"2.0","id":1,"stream":"item","seq":1,"data":1,"error":{"code":-1,"message":"x"}}"#,
    );
    assert!(frame.is_err(), "Item must reject unknown `error` field");
}

#[test]
fn stream_error_without_error_field_rejected() {
    let frame = peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"stream":"error"}"#);
    assert!(frame.is_err(), "Error must require `error`");
}

#[test]
fn stream_error_with_extra_data_field_rejected() {
    let frame = peer::parse_frame(
        r#"{"jsonrpc":"2.0","id":1,"stream":"error","error":{"code":-1,"message":"x"},"data":42}"#,
    );
    assert!(frame.is_err(), "Error must reject unknown `data` field");
}

#[test]
fn stream_close_with_payload_rejected() {
    assert!(peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"stream":"close","data":1}"#).is_err());
    assert!(
        peer::parse_frame(
            r#"{"jsonrpc":"2.0","id":1,"stream":"close","error":{"code":-1,"message":"x"}}"#
        )
        .is_err()
    );
}

#[test]
fn stream_cancel_with_payload_rejected() {
    assert!(peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"stream":"cancel","data":1}"#).is_err());
}

#[test]
fn stream_with_unknown_phase_rejected() {
    // "transfer" is not a recognized stream phase.
    assert!(peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"stream":"transfer"}"#).is_err());
}

// ---------------------------------------------------------------------------
// Frame disambiguation — Stream variant
// ---------------------------------------------------------------------------

#[test]
fn parse_frame_classifies_stream_open() {
    let frame = peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"stream":"open"}"#).unwrap();
    assert!(matches!(frame, Frame::Stream(StreamFrame::Open { .. })));
}

#[test]
fn parse_frame_classifies_stream_item() {
    let frame =
        peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"stream":"item","seq":42,"data":[1,2,3]}"#)
            .unwrap();
    match frame {
        Frame::Stream(StreamFrame::Item { seq, data, .. }) => {
            assert_eq!(seq, 42);
            assert_eq!(data, json!([1, 2, 3]));
        }
        other => panic!("expected Item, got {other:?}"),
    }
}

#[test]
fn parse_frame_classifies_stream_close_error_cancel() {
    let close = peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"stream":"close"}"#).unwrap();
    assert!(matches!(close, Frame::Stream(StreamFrame::Close { .. })));

    let error = peer::parse_frame(
        r#"{"jsonrpc":"2.0","id":1,"stream":"error","error":{"code":-1,"message":"x"}}"#,
    )
    .unwrap();
    assert!(matches!(error, Frame::Stream(StreamFrame::Error { .. })));

    let cancel = peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"stream":"cancel"}"#).unwrap();
    assert!(matches!(cancel, Frame::Stream(StreamFrame::Cancel { .. })));
}

// ---------------------------------------------------------------------------
// classify — InboundKind::Stream
// ---------------------------------------------------------------------------

#[test]
fn classify_stream_yields_stream_kind() {
    let frame =
        peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"stream":"item","seq":3,"data":{"k":"v"}}"#)
            .unwrap();
    match peer::classify(frame) {
        InboundKind::Stream(StreamFrame::Item { id, seq, data, .. }) => {
            assert_eq!(id, Id::Number(1));
            assert_eq!(seq, 3);
            assert_eq!(data, json!({"k": "v"}));
        }
        other => panic!("expected Stream::Item, got {other:?}"),
    }
}

#[test]
fn parse_frame_accepts_bad_version_on_stream() {
    // Parse no longer rejects bad jsonrpc versions — the field is
    // fully optional. Callers who care can validate themselves.
    let frame = peer::parse_frame(r#"{"jsonrpc":"1.0","id":1,"stream":"open"}"#).unwrap();
    assert!(matches!(frame, Frame::Stream(_)));
}

// ---------------------------------------------------------------------------
// End-to-end lifecycle — open → items → close (single direction)
// ---------------------------------------------------------------------------

#[test]
fn server_stream_lifecycle_open_items_close() {
    let id = Id::Number(42);
    let pushed = vec![
        peer::stream_open(id.clone()),
        peer::stream_item(id.clone(), 1, &json!(1)).unwrap(),
        peer::stream_item(id.clone(), 2, &json!(2)).unwrap(),
        peer::stream_close(id.clone()),
    ];

    let mut phase_tags = Vec::new();
    let mut items: Vec<serde_json::Value> = Vec::new();
    for sf in pushed {
        let wire = serde_json::to_string(&Frame::Stream(sf)).unwrap();
        match peer::classify(peer::parse_frame(&wire).unwrap()) {
            InboundKind::Stream(s) => {
                assert_eq!(s.id(), &id);
                match s {
                    StreamFrame::Open { .. } => phase_tags.push("open"),
                    StreamFrame::Item { data, .. } => {
                        phase_tags.push("item");
                        items.push(data);
                    }
                    StreamFrame::Close { .. } => phase_tags.push("close"),
                    StreamFrame::Error { .. } => phase_tags.push("error"),
                    StreamFrame::Cancel { .. } => phase_tags.push("cancel"),
                }
            }
            other => panic!("expected Stream, got {other:?}"),
        }
    }

    assert_eq!(phase_tags, vec!["open", "item", "item", "close"]);
    assert_eq!(items, vec![json!(1), json!(2)]);
}

#[test]
fn client_can_cancel_via_stream_cancel_from_either_side() {
    let cancel_from_client = peer::stream_cancel(99u64);
    let wire = serde_json::to_string(&Frame::Stream(cancel_from_client)).unwrap();
    match peer::classify(peer::parse_frame(&wire).unwrap()) {
        InboundKind::Stream(StreamFrame::Cancel { id, .. }) => {
            assert_eq!(id, Id::Number(99));
        }
        other => panic!("expected Cancel, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Wire disambiguation: Stream variant doesn't collide with Response /
// Request / Notification
// ---------------------------------------------------------------------------

#[test]
fn stream_does_not_match_response_or_request_shapes() {
    let resp_frame = peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"result":"v"}"#).unwrap();
    assert!(matches!(resp_frame, Frame::Response(_)));

    let req_frame = peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"method":"x"}"#).unwrap();
    assert!(matches!(req_frame, Frame::Request(_)));
}

#[test]
fn request_with_stream_field_is_rejected() {
    // Request's deny_unknown_fields rejects stream; only Frame::Stream
    // should match it.
    let frame = peer::parse_frame(r#"{"jsonrpc":"2.0","id":1,"stream":"open"}"#).unwrap();
    assert!(matches!(frame, Frame::Stream(_)));
}
