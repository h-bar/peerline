# Outbound Stream Fairness Plan

## Problem

Peerline already dispatches inbound handlers concurrently with `FuturesUnordered`, so concurrent streaming requests can run at the same time. The delivery side is less fair: every response, notification, stream item, and terminal frame is serialized into one unbounded outbound FIFO through `send_frame`.

`StreamSender::send_item` is synchronous. A stream handler that loops over a large replay can enqueue a large number of frames before another handler gets scheduled. The writer then preserves FIFO order, which makes independent streams look like they are processed one-by-one from the client's perspective.

## Current Runtime Behavior

- `run_inbound` pushes request and stream-handler futures into `FuturesUnordered`.
- Each `StreamSender` owns only a stream id and a sequence counter.
- `send_item` serializes the item immediately and appends it to `PeerInner::outbound`.
- `forward_outbound` writes the single outbound queue to the transport sink in insertion order.
- `Peer::call_stream` documents cancel-on-drop, but `StreamReceiver::drop` only removes local receiver state; it does not send a cancel frame to the producer.

This is not a handler-concurrency limitation. It is outbound head-of-line queueing plus no producer backpressure.

## Proposed Runtime Direction

Introduce a structured outbound scheduler that can distinguish control frames from stream item frames before serialization.

Use a small high-priority control queue for unary responses, errors, notifications, and stream terminal frames. Use per-stream item queues for streaming payload frames. The writer drains control frames first, then walks active stream queues round-robin, preserving sequence order within each stream while preventing any one stream from monopolising the transport.

Keep the public frame schema stable for the scheduler work: this change is internal scheduling, not a JSON-RPC envelope change. The cancellation work below should use a reserved notification over the existing request/notification envelope; if a future implementation instead adds a new frame variant, that change needs explicit protocol versioning.

## Stream Sender API

Add an async send path:

```rust
impl StreamSender {
    pub async fn send_item_async<T: Serialize>(&self, data: &T) -> Result<(), Error>;
    pub fn try_send_item<T: Serialize>(&self, data: &T) -> Result<(), Error>;
}
```

`send_item_async` waits for per-stream queue capacity and gives the runtime real backpressure. `try_send_item` gives latency-sensitive callers an explicit non-waiting path. The current `send_item` can remain as a compatibility wrapper backed by a default queue policy, but new high-volume producers should move to the async API.

Queue capacity should be configurable on `Peer` construction. The default can preserve current behavior during migration, then a bounded default can be adopted once downstream users have moved hot loops to `send_item_async`.

## Cancellation

Make the documented cancel-on-drop behavior real.

When `StreamReceiver` is dropped before terminal receipt, send a reserved `stream:cancel` notification carrying the stream id and remove the local registry entry. The producing peer intercepts that notification before user notification handlers, routes it to the active stream handler, closes the per-stream queue, and makes subsequent sends return `Error::Closed`.

Internally, each stream handler should receive a cancellation token owned by the runtime. The first implementation can expose cancellation only through `StreamSender` failures; a later API can expose an explicit `cancelled()` future for handlers that want to stop expensive upstream work before the next send.

## Metrics

Extend `Metrics` with enough visibility to operate bounded queues:

- per-stream queued item count, at least aggregated as max and total
- cancelled stream count
- send backpressure wait count or cumulative wait time
- scheduler rounds or fairness yield count

These values let applications detect a slow transport or a client that opens streams and stops reading.

## Compatibility

Per-stream sequence numbers stay unchanged. A stream's own item order remains stable, including gaps created through `StreamSender::skip`.

Unary request/response ordering is not currently specified relative to stream items and should remain unspecified. Prioritising control frames may change cross-stream and response-vs-item interleaving, but it improves latency without violating per-stream ordering.

Existing users can continue calling `send_item`. High-volume streams should adopt `send_item_async` once available to avoid filling memory under a slow transport.

## Validation

Add loopback runtime tests with two stream handlers:

- One handler sends a large number of items and another sends a small number; the receiver observes interleaving before the large stream completes.
- Per-stream sequence order remains monotonic under interleaving.
- Dropping a receiver causes the producer's subsequent send to fail.
- Bounded queue mode applies backpressure instead of growing `outbound_depth` without limit.

## Explicit Exclusions

This plan does not require changing peerline's public wire frame schema if cancellation is encoded as a reserved notification. It also does not define application-level batching; applications such as coagent can still batch database rows before calling peerline.
