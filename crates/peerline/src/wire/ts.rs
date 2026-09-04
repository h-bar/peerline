//! Wire-view mirrors for TypeScript export (`ts-export` feature only).
//!
//! [`Frame`](crate::wire::Frame)'s (de)serialization is hand-written —
//! the two-level `ver` / `kind` tagging funnelled through
//! [`WireV1`](crate::wire::v1) — so deriving `TS` on
//! [`Content`](crate::wire::Content) / [`Request`](crate::wire::Request)
//! / … would emit shapes that don't match the wire. Instead this module
//! declares one flat mirror struct per envelope that states the *wire*
//! shape literally, with `#[ts(type = ...)]` for the literal `ver` /
//! `kind` tags, and wraps the five in an untagged [`WireFrame`] union so
//! ts-rs emits `type WireFrame = WireRequest | …`.
//!
//! These types exist **only** to be exported; nothing serializes through
//! them. `tests/ts_mirror.rs` serializes real [`Frame`](crate::wire::Frame)s
//! and checks their JSON keys against each mirror's *generated TypeScript
//! declaration*, so a mirror cannot silently drift from the wire.
//!
//! ### Id width
//!
//! Wire ids are `u64` in Rust; the TypeScript port uses `number`
//! (exact to 2^53, and ids are allocated locally from 1 by each peer, so
//! the ceiling is unreachable in practice). The `id` / `seq` fields carry
//! an explicit `#[ts(type = "number")]` so the exported contract is
//! pinned regardless of how the ts-rs version in use maps `u64` / `i64`.

use serde_json::{Map, Value};
use ts_rs::TS;

// ---------------------------------------------------------------------------
// Mirrors — one per envelope
// ---------------------------------------------------------------------------

/// `{"ver":"1","kind":"req","id":7,"op":"foo","args":{…}}` — a call that
/// expects a reply. `args` is omitted entirely when the request carries
/// no arguments.
#[derive(TS)]
#[ts(export)]
pub struct WireRequest {
    /// Wire version tag — always the literal `"1"`.
    #[ts(type = "\"1\"")]
    pub ver: (),
    /// Envelope tag.
    #[ts(type = "\"req\"")]
    pub kind: (),
    /// Caller-chosen request id.
    #[ts(type = "number")]
    pub id: u64,
    /// The operation being invoked.
    pub op: String,
    /// Operation arguments — a JSON object, key absent when there are none.
    #[ts(optional)]
    pub args: Option<Map<String, Value>>,
}

/// `{"ver":"1","kind":"resp","id":7,"data":42}` — a successful reply.
///
/// The `data` key is **required**: it is emitted even when the payload is
/// `null` (a unit result), and an absent `data` with no `err` is a
/// protocol error, not an empty success.
#[derive(TS)]
#[ts(export)]
pub struct WireResponseOk {
    /// Wire version tag — always the literal `"1"`.
    #[ts(type = "\"1\"")]
    pub ver: (),
    /// Envelope tag.
    #[ts(type = "\"resp\"")]
    pub kind: (),
    /// Echoes the request's id — never null on a success reply.
    #[ts(type = "number")]
    pub id: u64,
    /// Success payload. Present even when `null`.
    #[ts(type = "unknown")]
    pub data: Value,
}

/// `{"ver":"1","kind":"resp","id":7,"err":{…}}` — a failed reply.
///
/// The `id` key is required but may be an explicit `null`, used when the
/// responder couldn't recover the request id from a malformed frame.
#[derive(TS)]
#[ts(export)]
pub struct WireResponseErr {
    /// Wire version tag — always the literal `"1"`.
    #[ts(type = "\"1\"")]
    pub ver: (),
    /// Envelope tag.
    #[ts(type = "\"resp\"")]
    pub kind: (),
    /// Echoes the request's id, or `null` if it couldn't be recovered.
    #[ts(type = "number | null")]
    pub id: Option<u64>,
    /// Error payload.
    pub err: crate::wire::RpcError,
}

/// `{"ver":"1","kind":"notif","op":"event","args":{…}}` — a one-way call.
/// Carries no `id`; `args` is omitted when there are no arguments.
#[derive(TS)]
#[ts(export)]
pub struct WireNotification {
    /// Wire version tag — always the literal `"1"`.
    #[ts(type = "\"1\"")]
    pub ver: (),
    /// Envelope tag.
    #[ts(type = "\"notif\"")]
    pub kind: (),
    /// The operation name.
    pub op: String,
    /// Notification arguments — a JSON object, key absent when there are none.
    #[ts(optional)]
    pub args: Option<Map<String, Value>>,
}

/// `{"ver":"1","kind":"stream","id":7,"seq":0,"data":{…}}` — one stream
/// frame, correlated to the originating request by `id`.
///
/// Lifecycle lives in `seq`: `>= 0` is a regular item (0-indexed), `-1`
/// is the terminal frame. Both `data` and `err` are optional — a
/// terminal may carry the last item, an error, both, or neither.
#[derive(TS)]
#[ts(export)]
pub struct WireStreamFrame {
    /// Wire version tag — always the literal `"1"`.
    #[ts(type = "\"1\"")]
    pub ver: (),
    /// Envelope tag.
    #[ts(type = "\"stream\"")]
    pub kind: (),
    /// Correlates to the originating request.
    #[ts(type = "number")]
    pub id: u64,
    /// Sequence number: `>= 0` for items, `-1` for the terminal frame.
    #[ts(type = "number")]
    pub seq: i64,
    /// Stream element payload — key absent when the frame carries none.
    #[ts(optional, type = "unknown")]
    pub data: Option<Value>,
    /// Error payload — key absent unless the stream ended in error.
    #[ts(optional)]
    pub err: Option<crate::wire::RpcError>,
}

// ---------------------------------------------------------------------------
// Union
// ---------------------------------------------------------------------------

/// Any v1 frame. ts-rs won't synthesise a union from independent
/// structs, so the five mirrors are wrapped in an untagged enum, which
/// exports as `type WireFrame = WireRequest | WireResponseOk | …`.
#[derive(TS)]
#[ts(export, untagged)]
pub enum WireFrame {
    /// See [`WireRequest`].
    Request(WireRequest),
    /// See [`WireResponseOk`].
    ResponseOk(WireResponseOk),
    /// See [`WireResponseErr`].
    ResponseErr(WireResponseErr),
    /// See [`WireNotification`].
    Notification(WireNotification),
    /// See [`WireStreamFrame`].
    StreamFrame(WireStreamFrame),
}

// ---------------------------------------------------------------------------
// Declaration introspection — used by the mirror-drift test
// ---------------------------------------------------------------------------

/// One field as it appears in a mirror's *generated TypeScript*
/// declaration. Produced by [`mirror_fields`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorField {
    /// The wire key.
    pub name: String,
    /// `true` when the key is declared optional (`name?: …`), i.e. the
    /// key may be absent from the frame.
    pub optional: bool,
    /// The declared TypeScript type, verbatim (e.g. `"1"`, `number`,
    /// `RpcError`).
    pub ty: String,
}

/// Parse a mirror's generated TypeScript object literal into its
/// top-level fields.
///
/// Reads `T::inline()` — the same text ts-rs writes into the exported
/// `.ts` file — so the mirror-drift test compares real frames against
/// the artifact the TypeScript package actually consumes, not against a
/// hand-maintained key list that could itself drift.
///
/// # Panics
///
/// Panics if `T`'s inline declaration isn't a `{ … }` object literal.
#[must_use]
pub fn mirror_fields<T: TS>() -> Vec<MirrorField> {
    parse_object_literal(&T::inline(&ts_rs::Config::default()))
}

/// `T`'s generated TypeScript declaration, verbatim — the right-hand
/// side of the `export type …` line in the exported `.ts` file. Used by
/// the mirror-drift test to check the [`WireFrame`] union's members.
#[must_use]
pub fn mirror_decl<T: TS>() -> String {
    T::decl(&ts_rs::Config::default())
}

/// Remove `/* … */` and `// …` comments, leaving string literals alone.
fn strip_comments(src: &str) -> String {
    let chars: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(src.len());
    let mut i = 0usize;
    while i < chars.len() {
        match chars[i] {
            q @ ('"' | '\'' | '`') => {
                out.push(q);
                i += 1;
                while i < chars.len() && chars[i] != q {
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        out.push(chars[i]);
                        i += 1;
                    }
                    out.push(chars[i]);
                    i += 1;
                }
                if i < chars.len() {
                    out.push(q);
                    i += 1;
                }
            }
            '/' if chars.get(i + 1) == Some(&'*') => {
                i += 2;
                while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                    i += 1;
                }
                i = (i + 2).min(chars.len());
                out.push(' ');
            }
            '/' if chars.get(i + 1) == Some(&'/') => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

/// Scan a TypeScript object literal for its top-level `name: type` /
/// `name?: type` members, tracking nesting depth and string literals so
/// nested objects and quoted types (`"req"`) don't confuse the split.
fn parse_object_literal(decl: &str) -> Vec<MirrorField> {
    // ts-rs carries the Rust doc comments into the declaration as
    // `/** … */` blocks; strip them so their prose can't look like a
    // member.
    let stripped = strip_comments(decl);
    let src: Vec<char> = stripped.trim().chars().collect();
    assert!(
        src.first() == Some(&'{') && src.last() == Some(&'}'),
        "expected an object literal, got: {decl}"
    );

    let mut fields = Vec::new();
    let mut depth = 0usize;
    let mut i = 0usize;
    // At depth 1 the parser alternates between "a key may start here"
    // and "we're inside a type"; `,` at depth 1 returns it to the former.
    let mut expect_key = false;

    while i < src.len() {
        let c = src[i];
        match c {
            '"' | '\'' | '`' => {
                // Skip the whole string literal (handles escapes).
                let quote = c;
                i += 1;
                while i < src.len() && src[i] != quote {
                    i += if src[i] == '\\' { 2 } else { 1 };
                }
            }
            '{' | '[' | '(' | '<' => {
                depth += 1;
                if depth == 1 {
                    expect_key = true;
                }
            }
            '}' | ']' | ')' | '>' => depth = depth.saturating_sub(1),
            ',' if depth == 1 => expect_key = true,
            c if expect_key && (c.is_alphanumeric() || c == '_' || c == '$') => {
                let start = i;
                while i < src.len() && (src[i].is_alphanumeric() || src[i] == '_' || src[i] == '$')
                {
                    i += 1;
                }
                let name: String = src[start..i].iter().collect();
                while i < src.len() && src[i].is_whitespace() {
                    i += 1;
                }
                let optional = src.get(i) == Some(&'?');
                if optional {
                    i += 1;
                }
                while i < src.len() && src[i].is_whitespace() {
                    i += 1;
                }
                assert_eq!(src.get(i), Some(&':'), "malformed member in: {decl}");
                let ty_start = i + 1;
                let ty_end = scan_type(&src, ty_start);
                let ty: String = src[ty_start..ty_end].iter().collect();
                fields.push(MirrorField {
                    name,
                    optional,
                    ty: ty.trim().to_owned(),
                });
                expect_key = false;
                i = ty_end;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    fields
}

/// Index of the end of the type expression starting at `from` — the
/// next depth-1 `,` or the closing `}` of the object literal.
fn scan_type(src: &[char], from: usize) -> usize {
    let mut depth = 0usize;
    let mut i = from;
    while i < src.len() {
        match src[i] {
            '"' | '\'' | '`' => {
                let quote = src[i];
                i += 1;
                while i < src.len() && src[i] != quote {
                    i += if src[i] == '\\' { 2 } else { 1 };
                }
            }
            '{' | '[' | '(' | '<' => depth += 1,
            '}' | ']' | ')' | '>' if depth == 0 => return i,
            '}' | ']' | ')' | '>' => depth -= 1,
            ',' if depth == 0 => return i,
            _ => {}
        }
        i += 1;
    }
    i
}
