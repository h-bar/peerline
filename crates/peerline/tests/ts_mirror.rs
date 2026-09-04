//! Mirror-drift guard for the ts-rs wire-view mirrors.
//!
//! [`peerline::wire::ts`] declares one flat struct per envelope stating
//! the *wire* shape literally, because `Frame`'s (de)serialization is
//! hand-written and can't be derived from. That's a hand-maintained
//! restatement of the wire contract, so it can drift.
//!
//! This test closes the gap mechanically: it serializes **real** frames
//! (built through the public `peer` builders, serialized by `Frame`'s
//! own `Serialize`) and checks each frame's JSON object against the
//! matching mirror's *generated TypeScript declaration* — not against a
//! hand-written key list, which would just be a second thing to drift.
//! Three properties per frame:
//!
//! 1. every wire key is declared by the mirror (no undeclared keys);
//! 2. every key the mirror declares as required is present;
//! 3. every key the mirror types as a string literal (`ver` / `kind`)
//!    carries exactly that literal on the wire.

#![cfg(feature = "ts-export")]

use peerline::peer;
use peerline::wire::ts::{
    MirrorField, WireFrame, WireNotification, WireRequest, WireResponseErr, WireResponseOk,
    WireStreamFrame, mirror_decl, mirror_fields,
};
use peerline::wire::{Frame, RpcError};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Serialize `frame` and check it against `declared` (a mirror's
/// generated field list).
fn check(label: &str, frame: impl Into<Frame>, declared: &[MirrorField]) -> Value {
    let frame: Frame = frame.into();
    let value = serde_json::to_value(&frame).expect("frame serializes");
    let obj = value
        .as_object()
        .unwrap_or_else(|| panic!("{label}: frame did not serialize to a JSON object"));

    assert!(!declared.is_empty(), "{label}: mirror declared no fields");

    // 1. no undeclared wire keys.
    for key in obj.keys() {
        assert!(
            declared.iter().any(|f| &f.name == key),
            "{label}: wire key `{key}` is not declared by the mirror \
             (declared: {:?})",
            declared.iter().map(|f| &f.name).collect::<Vec<_>>()
        );
    }

    // 2. every required declared key is on the wire.
    for field in declared.iter().filter(|f| !f.optional) {
        assert!(
            obj.contains_key(&field.name),
            "{label}: mirror declares `{}` as required but the frame omits it",
            field.name
        );
    }

    // 3. literal tags match.
    for field in declared.iter().filter(|f| f.ty.starts_with('"')) {
        let actual = obj
            .get(&field.name)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{label}: `{}` is not a wire string", field.name));
        assert_eq!(
            field.ty,
            format!("\"{actual}\""),
            "{label}: mirror types `{}` as {} but the wire carries {actual:?}",
            field.name,
            field.ty
        );
    }

    value
}

/// The mirror's declaration of one key, by name.
fn field<'a>(declared: &'a [MirrorField], name: &str) -> &'a MirrorField {
    declared
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("mirror declares no `{name}` field"))
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

#[test]
fn request_mirror_matches_the_wire() {
    let declared = mirror_fields::<WireRequest>();

    let with_args = check(
        "request with args",
        peer::request(7u64, "foo", &json!({"x": 1})).unwrap(),
        &declared,
    );
    assert!(with_args.get("args").is_some());

    let no_args = check(
        "request without args",
        peer::request_no_args(8u64, "foo"),
        &declared,
    );
    assert!(
        no_args.get("args").is_none(),
        "an argument-less request omits the `args` key entirely"
    );
    assert!(
        field(&declared, "args").optional,
        "`args` must be declared optional — it is absent on argument-less requests"
    );
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

#[test]
fn ok_response_mirror_matches_the_wire() {
    let declared = mirror_fields::<WireResponseOk>();

    check(
        "ok response",
        peer::response_ok(7u64, &json!({"sum": 3})).unwrap(),
        &declared,
    );

    // A unit result serializes to `"data": null` — the key is PRESENT.
    // Absent `data` with no `err` is a protocol error, not an empty
    // success, so the mirror must declare `data` required.
    let unit = check(
        "ok response with null data",
        peer::response_ok(7u64, &()).unwrap(),
        &declared,
    );
    assert_eq!(unit.get("data"), Some(&Value::Null));
    assert!(
        !field(&declared, "data").optional,
        "`data` on an ok response is required — it is emitted even when null"
    );
}

#[test]
fn err_response_mirror_matches_the_wire() {
    let declared = mirror_fields::<WireResponseErr>();

    check(
        "err response",
        peer::response_err(Some(7u64), -32601, "no such method"),
        &declared,
    );

    // The `id` key is required but may be an explicit null.
    let null_id = check(
        "err response with null id",
        peer::response_err(None, -32600, "invalid frame"),
        &declared,
    );
    assert_eq!(null_id.get("id"), Some(&Value::Null));
    assert!(
        !field(&declared, "id").optional,
        "`id` on an error response is required (numeric or explicit null)"
    );
    assert_eq!(field(&declared, "id").ty, "number | null");

    // An error carrying `data` — relayed verbatim by the runtime.
    let with_data = check(
        "err response carrying data",
        peer::response_err_with_data(Some(9u64), -32050, "boom", json!({"why": "test"})),
        &declared,
    );
    assert_eq!(
        with_data["err"]["data"],
        json!({"why": "test"}),
        "RpcError::data must survive serialization"
    );
}

#[test]
fn rpc_error_mirror_matches_the_wire() {
    // `RpcError` is a direct derive (its serde impl *is* the wire shape),
    // so it is checked field-by-field rather than through a mirror.
    let declared = mirror_fields::<RpcError>();
    let names: Vec<&str> = declared.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["code", "msg", "data"]);

    // `data` is skipped when None — so the key must be optional, NOT
    // `data: JsonValue | null`. (ts-rs's default Option mapping is the
    // latter; the explicit `#[ts(optional)]` is what makes it right.)
    assert!(field(&declared, "data").optional);
    assert!(!field(&declared, "code").optional);
    assert!(!field(&declared, "msg").optional);

    let bare = serde_json::to_value(RpcError {
        code: -1,
        message: "x".into(),
        data: None,
    })
    .unwrap();
    assert!(
        bare.get("data").is_none(),
        "an RpcError without data omits the key"
    );
    assert!(bare.get("msg").is_some(), "`message` is renamed to `msg`");
}

// ---------------------------------------------------------------------------
// Notifications
// ---------------------------------------------------------------------------

#[test]
fn notification_mirror_matches_the_wire() {
    let declared = mirror_fields::<WireNotification>();

    check(
        "notification with args",
        peer::notification("event", &json!({"n": 1})).unwrap(),
        &declared,
    );

    let no_args = check(
        "notification without args",
        peer::notification_no_args("tick"),
        &declared,
    );
    assert!(no_args.get("args").is_none());
    assert!(
        no_args.get("id").is_none(),
        "notifications carry no id — and the mirror declares none"
    );
    assert!(declared.iter().all(|f| f.name != "id"));
}

// ---------------------------------------------------------------------------
// Stream frames
// ---------------------------------------------------------------------------

#[test]
fn stream_frame_mirror_matches_the_wire() {
    let declared = mirror_fields::<WireStreamFrame>();

    let item = check(
        "stream item",
        peer::stream_item(7u64, 0, &json!({"line": "a"})).unwrap(),
        &declared,
    );
    assert_eq!(item["seq"], json!(0));

    let terminal = check("empty terminal", peer::stream_terminal(7u64), &declared);
    assert_eq!(terminal["seq"], json!(-1));
    assert!(terminal.get("data").is_none());
    assert!(terminal.get("err").is_none());
    assert!(field(&declared, "data").optional);
    assert!(field(&declared, "err").optional);

    let with_data = check(
        "terminal carrying the last item",
        peer::stream_terminal_with_data(7u64, &json!({"line": "z"})).unwrap(),
        &declared,
    );
    assert!(with_data.get("data").is_some());

    let with_null_data = check(
        "terminal carrying null data",
        peer::stream_terminal_with_data(7u64, &()).unwrap(),
        &declared,
    );
    assert_eq!(with_null_data.get("data"), Some(&Value::Null));

    check(
        "terminal carrying an error",
        peer::stream_terminal_with_error(7u64, -32050, "boom"),
        &declared,
    );

    let rpc_err = check(
        "terminal carrying a full RpcError",
        peer::stream_terminal_with_rpc_error(
            7u64,
            RpcError {
                code: -32050,
                message: "boom".into(),
                data: Some(json!({"at": 3})),
            },
        ),
        &declared,
    );
    assert_eq!(rpc_err["err"]["data"], json!({"at": 3}));

    check(
        "terminal carrying an error with data",
        peer::stream_terminal_with_error_data(7u64, -32050, "boom", json!({"at": 3})),
        &declared,
    );
}

// ---------------------------------------------------------------------------
// The union
// ---------------------------------------------------------------------------

#[test]
fn wire_frame_union_covers_every_envelope() {
    let decl = mirror_decl::<WireFrame>();
    for member in [
        "WireRequest",
        "WireResponseOk",
        "WireResponseErr",
        "WireNotification",
        "WireStreamFrame",
    ] {
        assert!(
            decl.contains(member),
            "WireFrame union is missing {member}: {decl}"
        );
    }
    // Untagged union, not a tagged-enum object shape.
    assert!(
        !decl.contains('{'),
        "WireFrame must export as a bare union of the mirrors: {decl}"
    );
    assert_eq!(
        decl.matches('|').count(),
        4,
        "WireFrame should union exactly the five envelope mirrors: {decl}"
    );
}
