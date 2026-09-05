//! peerline pubsub layer — server-pushed event streams over
//! notifications.
//!
//! Layers cleanly on top of [`crate::wire`] / [`crate::peer`]
//! without modifying them. Under peerline's peer-symmetric model,
//! either peer can send a [`Notification`] — so subscription pushes
//! are just notifications on the reserved [`EVENT_OP`] / [`END_OP`]
//! op names. No separate "event" wire type is needed.
//!
//! ### Wire conventions
//!
//! - Subscribe RPC (application-named, e.g. `"subscribe"`) returns
//!   [`SubscriptionAck`] in `result`.
//! - The pushing peer emits [`Notification`] frames with
//!   `op == `[`EVENT_OP`] carrying [`EventParams`] (per-subscription
//!   event), and optionally one `op == `[`END_OP`] with [`EndParams`]
//!   when a bounded stream completes.
//! - The receiving peer cancels with an [`UNSUBSCRIBE_OP`] request
//!   whose params are [`UnsubscribeParams`].
//!
//! ### Client-side classification
//!
//! Pipe any [`InboundKind::IncomingNotification`](crate::peer::InboundKind::IncomingNotification)
//! through [`classify`] to recognise pubsub messages.

use crate::peer;
use crate::wire::{Id, Notification, Params, Request};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Reserved op names
// ---------------------------------------------------------------------------
//
// `$peerline/`-prefixed, like the runtime's `$peerline/stream.cancel`,
// so they cannot collide with application op names — an app is free to
// have its own `event`, `end`, or `unsubscribe`. These are part of the
// cross-language wire contract: the TypeScript implementation and the
// conformance vectors pin the same literals, and any change here must
// land in both. (They were once the bare names `event`/`end`/
// `unsubscribe`; that spelling occupied the application namespace and
// was retired before anything external shipped against it.)

/// Op name of a pubsub event push.
pub const EVENT_OP: &str = "$peerline/pubsub.event";

/// Op name of the end-of-subscription push.
pub const END_OP: &str = "$peerline/pubsub.end";

/// Op name of the unsubscribe request. Unlike the pushes this is an
/// ordinary request the receiving peer answers — reserved-prefixed only
/// so the whole pubsub vocabulary stays out of the application's way.
pub const UNSUBSCRIBE_OP: &str = "$peerline/pubsub.unsubscribe";

// ---------------------------------------------------------------------------
// Wire envelopes
// ---------------------------------------------------------------------------

/// Body of the response to a subscribe call — the server-chosen
/// opaque id every subsequent push [`Notification`] carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-export", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-export", ts(export))]
pub struct SubscriptionAck {
    /// The subscription id the server allocated.
    pub subscription_id: String,
}

/// Params of an [`EVENT_OP`] notification: the subscription it belongs
/// to plus one event payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-export", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-export", ts(export))]
pub struct EventParams {
    /// Subscription this event belongs to.
    pub subscription_id: String,
    /// The event payload. Typed by the consumer.
    pub event: Value,
}

/// Params of an [`END_OP`] notification — sent once when a bounded
/// subscription stream completes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-export", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-export", ts(export))]
pub struct EndParams {
    /// Subscription that has ended.
    pub subscription_id: String,
}

/// Params of an [`UNSUBSCRIBE_OP`] request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-export", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-export", ts(export))]
pub struct UnsubscribeParams {
    /// Subscription to cancel.
    pub subscription_id: String,
}

// ---------------------------------------------------------------------------
// Server-side helpers (the pushing peer)
// ---------------------------------------------------------------------------

/// Build a [`SubscriptionAck`] payload — the response body a
/// subscribe handler returns.
#[must_use]
pub fn subscription_ack(subscription_id: impl Into<String>) -> SubscriptionAck {
    SubscriptionAck {
        subscription_id: subscription_id.into(),
    }
}

/// Build an [`EVENT_OP`] [`Notification`] wrapping the caller's
/// payload in an [`EventParams`] envelope.
pub fn event<T: Serialize>(
    subscription_id: impl Into<String>,
    payload: &T,
) -> Result<Notification, serde_json::Error> {
    let args = serde_json::to_value(EventParams {
        subscription_id: subscription_id.into(),
        event: serde_json::to_value(payload)?,
    })?;
    Ok(Notification {
        op: EVENT_OP.to_string(),
        args: params_from_value(args),
    })
}

/// Build an [`END_OP`] [`Notification`] marking the end of a bounded
/// subscription stream.
#[must_use]
pub fn end(subscription_id: impl Into<String>) -> Notification {
    let args = serde_json::to_value(EndParams {
        subscription_id: subscription_id.into(),
    })
    .unwrap_or(Value::Null);
    Notification {
        op: END_OP.to_string(),
        args: params_from_value(args),
    }
}

/// Server-allocated subscription id generator.
#[derive(Debug, Default)]
pub struct SubscriptionIdGen {
    next: AtomicU64,
}

impl SubscriptionIdGen {
    /// Fresh generator starting at `sub-0`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate the next subscription id.
    #[must_use]
    pub fn next(&self) -> String {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        format!("sub-{n}")
    }
}

// ---------------------------------------------------------------------------
// Client-side helpers (the receiving peer)
// ---------------------------------------------------------------------------

/// Build an [`UNSUBSCRIBE_OP`] request for the given subscription id.
#[must_use]
pub fn unsubscribe_request(id: impl Into<Id>, subscription_id: impl Into<String>) -> Request {
    peer::request(
        id,
        UNSUBSCRIBE_OP,
        &UnsubscribeParams {
            subscription_id: subscription_id.into(),
        },
    )
    .expect("UnsubscribeParams serializes to a JSON object")
}

/// A pubsub-method message, decoded by [`classify`].
#[derive(Debug, Clone, PartialEq)]
pub enum PubsubMessage {
    /// One event on a live subscription (op = [`EVENT_OP`]).
    Event {
        /// The subscription this event belongs to.
        subscription_id: String,
        /// Raw event payload — the consumer deserializes to the
        /// per-subscription event type.
        event: Value,
    },
    /// End-of-stream marker for a bounded subscription
    /// (op = [`END_OP`]).
    End {
        /// The subscription that has ended.
        subscription_id: String,
    },
}

/// Recognise a [`Notification`] as a pubsub [`EVENT_OP`] or [`END_OP`]
/// message. Returns `Some(PubsubMessage)` if the op matches and the
/// args deserialize cleanly; `None` otherwise.
///
/// The fields are read out of the args map directly rather than
/// through `serde_json::from_value` on a clone of it — this runs once
/// per delivered event, and cloning the whole map would double the
/// payload's allocation traffic just to throw the copy away.
pub fn classify(notif: &Notification) -> Option<PubsubMessage> {
    let is_event = notif.op == EVENT_OP;
    if !is_event && notif.op != END_OP {
        return None;
    }
    let args_obj = notif.args.as_ref()?;
    let subscription_id = args_obj.get("subscription_id")?.as_str()?.to_owned();
    if is_event {
        Some(PubsubMessage::Event {
            subscription_id,
            event: args_obj.get("event")?.clone(),
        })
    } else {
        Some(PubsubMessage::End { subscription_id })
    }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Convert a [`serde_json::Value`] (always an object for our
/// envelopes) into the typed [`Params`] field expected by
/// [`Notification`].
fn params_from_value(value: Value) -> Option<Params> {
    match value {
        Value::Object(o) => Some(o),
        Value::Null => Some(Map::new()),
        _ => None,
    }
}
