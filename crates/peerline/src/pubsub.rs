//! peerline pubsub layer — server-pushed event streams over
//! notifications.
//!
//! Layers cleanly on top of [`crate::wire`] / [`crate::peer`]
//! without modifying them. Under peerline's peer-symmetric model,
//! either peer can send a [`Notification`] — so subscription pushes
//! are just notifications on the `"event"` / `"end"` method names.
//! No separate "event" wire type is needed.
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
//! through [`classify`] to recognise pubsub messages.

use crate::peer;
use crate::wire::{Id, Notification, Params, Request};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::atomic::{AtomicU64, Ordering};

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

/// Params of an `event` notification: the subscription it belongs
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

/// Params of an `end` notification — sent once when a bounded
/// subscription stream completes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-export", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-export", ts(export))]
pub struct EndParams {
    /// Subscription that has ended.
    pub subscription_id: String,
}

/// Params of an `unsubscribe` request.
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

/// Build an `event` [`Notification`] wrapping the caller's payload
/// in an [`EventParams`] envelope.
pub fn event<T: Serialize>(
    subscription_id: impl Into<String>,
    payload: &T,
) -> Result<Notification, serde_json::Error> {
    let args = serde_json::to_value(EventParams {
        subscription_id: subscription_id.into(),
        event: serde_json::to_value(payload)?,
    })?;
    Ok(Notification {
        op: "event".to_string(),
        args: params_from_value(args),
    })
}

/// Build an `end` [`Notification`] marking the end of a bounded
/// subscription stream.
#[must_use]
pub fn end(subscription_id: impl Into<String>) -> Notification {
    let args = serde_json::to_value(EndParams {
        subscription_id: subscription_id.into(),
    })
    .unwrap_or(Value::Null);
    Notification {
        op: "end".to_string(),
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

/// Build an `unsubscribe` request for the given subscription id.
#[must_use]
pub fn unsubscribe_request(id: impl Into<Id>, subscription_id: impl Into<String>) -> Request {
    peer::request(
        id,
        "unsubscribe",
        &UnsubscribeParams {
            subscription_id: subscription_id.into(),
        },
    )
    .expect("UnsubscribeParams serializes to a JSON object")
}

/// A pubsub-method message, decoded by [`classify`].
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
/// message. Returns `Some(PubsubMessage)` if the op matches and the
/// args deserialize cleanly; `None` otherwise.
pub fn classify(notif: &Notification) -> Option<PubsubMessage> {
    let args_obj = notif.args.as_ref()?;
    let args_value = Value::Object(args_obj.clone());
    match notif.op.as_str() {
        "event" => {
            let e: EventParams = serde_json::from_value(args_value).ok()?;
            Some(PubsubMessage::Event {
                subscription_id: e.subscription_id,
                event: e.event,
            })
        }
        "end" => {
            let e: EndParams = serde_json::from_value(args_value).ok()?;
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
