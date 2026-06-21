//! JSON-RPC 2.0 pubsub-subscription extension — **not** in the spec.
//!
//! De-facto convention used by Ethereum's `eth_subscribe`,
//! jsonrpsee, and many other JSON-RPC frameworks. Layers cleanly on
//! top of the spec-aligned [`crate::wire`] / [`crate::peer`] modules
//! without modifying them.
//!
//! Under the peer-symmetric model, either peer can send a
//! [`crate::wire::Notification`] — so server-pushed subscription
//! events are just notifications on the `"event"` / `"end"` method
//! names. No separate "event" wire type is needed; the type-system
//! split between client-side and server-side notifications
//! dissolves.
//!
//! ### Wire conventions
//!
//! - Subscribe RPC (application-named, e.g. `"subscribe"`) returns
//!   [`SubscriptionAck`] in `result`.
//! - The pushing peer emits [`Notification`] frames with
//!   `method == "event"` carrying [`EventParams`] (per-subscription
//!   event), and optionally one `method == "end"` with [`EndParams`]
//!   when a bounded stream completes.
//! - The receiving peer cancels with an `unsubscribe` request whose
//!   params are [`UnsubscribeParams`].
//!
//! ### Client-side classification
//!
//! Pipe any [`InboundKind::IncomingNotification`](crate::peer::InboundKind::IncomingNotification)
//! through [`classify`] to recognise pubsub messages:
//!
//! ```ignore
//! match peer::classify(frame) {
//!     InboundKind::IncomingNotification(notif) => {
//!         match pubsub::classify(&notif) {
//!             Some(PubsubMessage::Event { subscription_id, event }) => …,
//!             Some(PubsubMessage::End { subscription_id }) => …,
//!             None => log_unknown_notification(notif),
//!         }
//!     }
//!     // …other InboundKind variants
//! }
//! ```

use crate::peer;
use crate::wire::{Id, JSONRPC_VERSION, Notification, Params, Request};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Wire envelopes
// ---------------------------------------------------------------------------

/// Body of the response to a subscribe call — the server-chosen
/// opaque id every subsequent push [`Notification`] carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionAck {
    /// The subscription id the server allocated.
    pub subscription_id: String,
}

/// Params of an `event` notification: the subscription it belongs
/// to plus one event payload. The event field is left as
/// [`serde_json::Value`] so a single connection can multiplex
/// subscriptions of different typed shapes; consumers deserialize
/// the value into their domain type per subscription.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventParams {
    /// Subscription this event belongs to.
    pub subscription_id: String,
    /// The event payload. Typed by the consumer.
    pub event: Value,
}

/// Params of an `end` notification — sent once when a bounded
/// subscription stream completes so the receiver can resolve its
/// receiver / async iterator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndParams {
    /// Subscription that has ended.
    pub subscription_id: String,
}

/// Params of an `unsubscribe` request — the id of the subscription
/// the caller wishes to cancel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsubscribeParams {
    /// Subscription to cancel.
    pub subscription_id: String,
}

// ---------------------------------------------------------------------------
// Server-side helpers (the pushing peer)
// ---------------------------------------------------------------------------

/// Build a [`SubscriptionAck`] payload — the response body a
/// subscribe handler returns. Wrap with
/// [`crate::peer::response_ok`] to produce the actual response.
#[must_use]
pub fn subscription_ack(subscription_id: impl Into<String>) -> SubscriptionAck {
    SubscriptionAck {
        subscription_id: subscription_id.into(),
    }
}

/// Build an `event` [`Notification`] for a subscription — wraps
/// the caller's typed `payload` in an [`EventParams`] envelope
/// under `method: "event"`. Errors only if the payload fails to
/// serialize.
pub fn event<T: Serialize>(
    subscription_id: impl Into<String>,
    payload: &T,
) -> Result<Notification, serde_json::Error> {
    let params_value = serde_json::to_value(EventParams {
        subscription_id: subscription_id.into(),
        event: serde_json::to_value(payload)?,
    })?;
    Ok(Notification {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        method: "event".to_string(),
        params: params_from_value(params_value),
    })
}

/// Build an `end` [`Notification`] marking the end of a bounded
/// subscription stream.
#[must_use]
pub fn end(subscription_id: impl Into<String>) -> Notification {
    let params_value = serde_json::to_value(EndParams {
        subscription_id: subscription_id.into(),
    })
    .unwrap_or(Value::Null);
    Notification {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        method: "end".to_string(),
        params: params_from_value(params_value),
    }
}

/// Server-allocated subscription id generator. Each call to
/// [`Self::next`] returns a fresh `sub-N` string. Backed by an
/// [`AtomicU64`] so a single instance can be shared across
/// concurrent subscribe handlers without external locking.
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

/// Build an `unsubscribe` request for the given subscription id —
/// convenience wrapper around [`crate::peer::request`] with the
/// standard `"unsubscribe"` method name and [`UnsubscribeParams`]
/// body.
#[must_use]
pub fn unsubscribe_request(id: impl Into<Id>, subscription_id: impl Into<String>) -> Request {
    // UnsubscribeParams always serializes to an object, so the
    // params-shape check inside `request` never fires here.
    peer::request(
        id,
        "unsubscribe",
        &UnsubscribeParams {
            subscription_id: subscription_id.into(),
        },
    )
    .expect("UnsubscribeParams serializes to a JSON object")
}

/// A pubsub-method message, decoded by [`classify`]. `None` from
/// `classify` means the notification's method isn't one this module
/// recognises.
#[derive(Debug, Clone, PartialEq)]
pub enum PubsubMessage {
    /// One event on a live subscription (method = `"event"`).
    Event {
        /// The subscription this event belongs to.
        subscription_id: String,
        /// Raw event payload — the consumer deserializes to the
        /// per-subscription event type.
        event: Value,
    },
    /// End-of-stream marker for a bounded subscription
    /// (method = `"end"`).
    End {
        /// The subscription that has ended.
        subscription_id: String,
    },
}

/// Recognise a [`Notification`] as a pubsub `event` or `end`
/// message. Returns `Some(PubsubMessage)` if the method matches and
/// the params deserialize cleanly; `None` otherwise — leaves the
/// original notification untouched for the caller to handle (log,
/// pass to another extension classifier, …).
pub fn classify(notif: &Notification) -> Option<PubsubMessage> {
    let params_value = notif.params.as_ref()?.as_value();
    match notif.method.as_str() {
        "event" => {
            let e: EventParams = serde_json::from_value(params_value).ok()?;
            Some(PubsubMessage::Event {
                subscription_id: e.subscription_id,
                event: e.event,
            })
        }
        "end" => {
            let e: EndParams = serde_json::from_value(params_value).ok()?;
            Some(PubsubMessage::End {
                subscription_id: e.subscription_id,
            })
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Convert a `serde_json::Value` we just constructed (always an
/// object for our envelopes) into the typed [`Params`] field
/// expected by [`Notification`].
fn params_from_value(value: Value) -> Option<Params> {
    match value {
        Value::Object(o) => Some(Params::Object(o)),
        Value::Array(a) => Some(Params::Array(a)),
        _ => None,
    }
}
