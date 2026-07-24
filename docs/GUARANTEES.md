# Delivery and ordering guarantees

What KPipe guarantees, the exact boundary of each guarantee, and the failure matrix. Everything on this page is
verified against `kpipe-consumer` 1.18.0 source; the machine-checkable invariants (jcstress + property suites) are
written down in [OFFSET-INVARIANTS.md](OFFSET-INVARIANTS.md).

## The headline guarantee

**At-least-once delivery under the documented configuration and failure model.** Concretely: a record's offset is
never committed until that record has reached a terminal state — sink completed, intentionally filtered, or durably
parked in the DLQ. If the process dies at any point before that, the record is redelivered after restart. The costs of
this guarantee are the standard ones: some records will be **reprocessed** after a crash or rebalance (your sink
should tolerate duplicates), and KPipe does **not** provide exactly-once semantics — there is no transactional
producer, no `sendOffsetsToTransaction`, no read-process-write EOS anywhere in the library.

Terminology used below:

- **tracked** — the offset is registered as in-flight, before processing starts.
- **processed / marked** — the record reached a terminal success state; the offset is eligible for commit.
- **committed** — sent to Kafka via `commitSync`. The committed value follows Kafka's next-to-read convention
  (processed offset + 1).
- **commit frontier** — the lowest offset per partition that is still in-flight; commits never pass it.

## Offset lifecycle

1. On poll, each record's offset is **tracked** before it is dispatched to a worker.
2. The worker runs deserialize → operators → sink as one unit. Only when that unit completes (or the record is
   filtered, or a DLQ send succeeds) is the offset **marked** processed.
3. A scheduler enqueues a commit every 30 seconds (default). The commit computes the frontier per partition — the
   lowest still-pending offset, or highest-processed + 1 when nothing is pending — and executes `commitSync` on the
   consumer thread. Rebalance revocation and shutdown also commit synchronously.
4. A failed commit is logged at WARNING and the ledger is untouched; the next interval (or the shutdown path) retries.
   Nothing is marked lost on a commit failure — the worst case is a wider reprocessing window.

Because of the frontier rule, offset 102 finishing before offset 101 does not let 102 commit; the frontier holds at
101 until it completes. This is what makes at-least-once true *with parallel processing* — and note it constrains
**commit order**, not side-effect order (see [Ordering](#ordering)).

## Failure matrix

| Situation | What happens | Redelivered on restart? |
| --- | --- | --- |
| Operator throws, retries remain | Record is retried from raw bytes after a fixed backoff (a mutated payload cannot poison the retry). `onFailed` fires per attempt. | n/a |
| Operator throws, retries exhausted | Error handler is invoked once; with a DLQ configured, the record is sent there. | Only if the DLQ send also fails |
| Deserialization fails | Same terminal path (retry → error handler / DLQ); `onFailed` does not observe it. | Only if the DLQ send also fails |
| Sink throws | Same terminal path; `onFailed` does not observe it. | Only if the DLQ send also fails |
| **DLQ send succeeds** | The record is durably parked; its offset is **marked processed** and commits. Successful DLQ publication counts as successful processing. | No — replay it from the DLQ topic if needed |
| **DLQ send fails** | The offset is left pending: it blocks the commit frontier and the record is redelivered on restart. A down DLQ applies backpressure instead of dropping records. `kpipe.producer.dlq.failed` increments. | Yes |
| No DLQ configured, retries exhausted | The error handler is the terminal step; the offset is then marked processed (log-and-advance is the explicit opt-in of not configuring a DLQ). | No |
| Error handler itself throws | Caught, logged; it cannot crash the worker, leak in-flight counts, or skip offset marking. | Per the underlying outcome |
| Record filtered (`filter` false / operator returns null) | Counts as success: offset marked, sink skipped, `onFiltered` fires. | No |
| Sink succeeded, crash before commit | The offset was marked but not yet committed — the record is **reprocessed** after restart. This is the at-least-once window; size it with the 30s commit interval in mind. | Yes |
| Rebalance revokes partitions | Revocation commits the current frontier synchronously; in-flight records for revoked partitions may still complete, and any not-yet-committed work is redelivered to the new assignee. | Possibly (standard at-least-once) |
| Graceful shutdown | In-flight records get a bounded drain (default 5s via `Handle.close()`); buffered batch sinks flush; a final synchronous commit runs before the consumer closes. Records that do not finish within the budget stay uncommitted. | Only those that missed the drain |
| Shutdown timeout exceeded | Remaining workers are interrupted; their offsets stay uncommitted. | Yes |
| `toMulti` fan-out, one sink throws | **Best effort by design:** the failure is logged and suppressed, other sinks run, the record counts as processed and commits. The DLQ is not invoked. Do not use `toMulti` where every sink must durably receive every record. | No |
| Unknown topic in `KPipe.multi(...)` (config error / rebalance race) | Dropped at WARNING and the offset **is committed** — a config error must not wedge the consumer into infinite refetch. | No |
| Interrupt during processing | Interruption is not retried; a coordinated shutdown begins. Offsets of interrupted records are not marked. | Yes |

## Ordering

Three distinct things, often conflated:

- **Kafka orders within a partition.** Records with the same key land on the same partition only if your *producer*
  partitions by key (the default partitioner does). KPipe cannot create ordering the topic doesn't have.
- **Processing order** is what `ProcessingMode` controls:
  - `PARALLEL` (default): no ordering. Each record gets its own virtual thread; side effects for offsets 5 and 6 can
    run concurrently or out of order.
  - `SEQUENTIAL`: strictly one record at a time for the whole consumer, in poll order (per-partition offset order,
    partitions interleaved). Use it when any cross-record ordering matters and throughput can afford it.
  - `KEY_ORDERED`: records sharing a key are processed serially in offset order; different keys run in parallel. Key
    equality is byte-equality of the Kafka key; all `null`-keyed records serialize through one queue. The dispatcher
    tracks up to `withKeyOrderedMaxKeys` distinct keys (default 10,000); at the cap, idle queues are evicted, and if
    every tracked key still has work queued, dispatch stalls the consumer thread until one drains (implicit
    backpressure, with a WARNING once per saturation episode).
- **Commit order** is the frontier rule above. It is a durability property, not an execution-ordering property.

So for "Authorize must apply before Capture for the same account": key the *producer* by account id (so both land on
one partition) **and** run KPipe in `KEY_ORDERED` (or `SEQUENTIAL`). Offset tracking alone does not give you this —
in `PARALLEL` mode the two side effects can interleave even though the commit frontier is correct.

## Backpressure

Enabled by default. When the monitored metric crosses the high watermark (default 10,000), KPipe pauses all assigned
partitions; when it drops to the low watermark (default 7,000), it resumes — hysteresis prevents oscillation. A paused
consumer **keeps polling**: paused partitions fetch nothing, but the poll keeps the consumer's group membership alive
(no `max.poll.interval.ms` eviction) and keeps the pause/resume decision on a fixed cadence. The monitored metric is
in-flight records for `PARALLEL`/`KEY_ORDERED` and consumer lag for `SEQUENTIAL` (where in-flight is always ≤ 1).

## Sinks

The guarantee boundary is the sink invocation: KPipe guarantees your sink is invoked at least once per non-filtered
record, and that the offset only commits afterward. What the sink does with the record — idempotency, its own
durability, partial writes — is outside KPipe's control. Two documented weakenings: `toMulti` is best-effort per sink
(above), and a batch sink's records are only as durable as the batch flush (the coverage contract and shutdown drain
are described in [SINKS.md](SINKS.md#batch-sinks)).

## External offset managers

With a custom `OffsetManager`, KPipe still guarantees the *call points* — track before dispatch, mark only on terminal
success, commit executed on the consumer thread — but the frontier logic itself (lowest-pending, no-commit-ahead)
lives in the default `KafkaOffsetManager`. A custom implementation takes on that responsibility; the invariants it
must uphold are specified in [OFFSET-INVARIANTS.md](OFFSET-INVARIANTS.md).

## What this is not

No exactly-once. No Kafka transactions. No guarantee that your side effects are idempotent — at-least-once explicitly
means your sink may see a record twice after a crash, and correctness under duplicates is the application's job.
