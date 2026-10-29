# Delivery and ordering guarantees

What KPipe guarantees, the exact boundary of each guarantee, and the failure matrix. Everything on this page is verified
against the `kpipe-consumer` source; the machine-checkable invariants (Fray + property suites) are written down in
[OFFSET-INVARIANTS.md](OFFSET-INVARIANTS.md).

## The headline guarantee

**At-least-once delivery under the documented configuration and failure model.** Concretely: a record's offset is never
committed until that record has reached a terminal state — sink completed, intentionally filtered, or durably parked in
the DLQ. If the process dies at any point before that, the record is redelivered after restart. The costs of this
guarantee are the standard ones: some records will be **reprocessed** after a crash or rebalance (your sink should
tolerate duplicates), and KPipe does **not** provide exactly-once semantics — there is no transactional producer, no
`sendOffsetsToTransaction`, no read-process-write EOS anywhere in the library.

Terminology used below:

- **tracked** — the offset is registered as in-flight, before processing starts.
- **processed / marked** — the record reached a terminal success state; the offset is eligible for commit.
- **committed** — sent to Kafka via `commitSync`. The committed value follows Kafka's next-to-read convention (processed
  offset + 1).
- **commit frontier** — the lowest offset per partition that is still in-flight; commits never pass it.

## Offset lifecycle

1. On poll, each record's offset is **tracked** before it is dispatched to a worker.
2. The worker runs deserialize → operators → sink as one unit. Only when that unit completes (or the record is filtered,
   or a DLQ send succeeds) is the offset **marked** processed.
3. A scheduler enqueues a commit every 30 seconds by default (`withCommitInterval`). The commit computes the frontier
   per partition — the lowest still-pending offset, or highest-processed + 1 when nothing is pending — and executes
   `commitSync` on the consumer thread. Rebalance revocation and shutdown also commit synchronously.
4. A failed commit is logged at WARNING and the ledger is untouched; the next interval (or the shutdown path) retries.
   Nothing is marked lost on a commit failure — the worst case is a wider reprocessing window.

Because of the frontier rule, offset 102 finishing before offset 101 does not let 102 commit; the frontier holds at 101
until it completes. This is what makes at-least-once true _with parallel processing_ — and note it constrains **commit
order**, not side-effect order (see [Ordering](#ordering)).

## Broker settings the guarantee depends on

A committed offset is only as durable as the `__consumer_offsets` partition that holds it, and a record parked in the
DLQ only as durable as the DLQ topic. A write sent with `acks=all` is acknowledged once every replica in the partition's
in-sync replica set has it, and `min.insync.replicas` is the floor: below it the broker refuses the write instead of
acknowledging it with fewer copies. Offset commits are always written this way. A DLQ send is covered only when its
producer uses `acks=all`, which is Kafka's default and what the DLQ producer KPipe builds uses unless the consumer's
properties set `acks`. Set `min.insync.replicas` to at least 2, with a replication factor of 3, for both topics.

In a KRaft cluster the effective value can come from a cluster-wide default rather than from each broker's own
configuration. On Kafka 4.3.0 with a dedicated controller left at its default, brokers configured with 2 still gave
every topic, `__consumer_offsets` included, a minimum of 1, sourced from `DYNAMIC_DEFAULT_BROKER_CONFIG`; with the
controller configured with 2 as well, every topic got 2. Check what a topic actually gets with
`kafka-configs.sh --bootstrap-server <broker> --entity-type topics --entity-name __consumer_offsets --describe --all`,
which shows `min.insync.replicas` and its source, and set the cluster-wide default with
`kafka-configs.sh --bootstrap-server <broker> --entity-type brokers --entity-default --alter --add-config min.insync.replicas=2`.

If the broker coordinating the consumer group stops answering without closing its connections, commits stall until the
client's request to it times out (`request.timeout.ms`, 30 seconds by default) and the client finds the new coordinator.
Nothing is lost meanwhile; a shorter timeout shortens the stall.

## Failure matrix

| Situation                                                           | What happens                                                                                                                                                                                                                    | Redelivered on restart?                     |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| Operator throws, retries remain                                     | Record is retried from raw bytes after a fixed backoff (a mutated payload cannot poison the retry). `onFailed` fires per attempt.                                                                                               | n/a                                         |
| Operator throws, retries exhausted                                  | Error handler is invoked once; with a DLQ configured, the record is sent there.                                                                                                                                                 | Only if the DLQ send also fails             |
| Deserialization fails                                               | Same terminal path (retry → error handler / DLQ); `onFailed` does not observe it.                                                                                                                                               | Only if the DLQ send also fails             |
| Sink throws                                                         | Same terminal path; `onFailed` does not observe it.                                                                                                                                                                             | Only if the DLQ send also fails             |
| **DLQ send succeeds**                                               | The record is durably parked; its offset is **marked processed** and commits. Successful DLQ publication counts as successful processing.                                                                                       | No — replay it from the DLQ topic if needed |
| **DLQ send fails**                                                  | The offset is left pending: it blocks the commit frontier and the record is redelivered on restart. A down DLQ applies backpressure instead of dropping records. `kpipe.producer.dlq.failed` increments.                        | Yes                                         |
| No DLQ configured, retries exhausted                                | The error handler is the terminal step; the offset is then marked processed (log-and-advance is the explicit opt-in of not configuring a DLQ).                                                                                  | No                                          |
| Error handler itself throws                                         | Caught, logged; it cannot crash the worker, leak in-flight counts, or skip offset marking.                                                                                                                                      | Per the underlying outcome                  |
| Record filtered (`filter` false / operator returns null)            | Counts as success: offset marked, sink skipped, `onFiltered` fires.                                                                                                                                                             | No                                          |
| Sink succeeded, crash before commit                                 | The offset was marked but not yet committed — the record is **reprocessed** after restart. This is the at-least-once window; its size is the commit interval, 30s by default (`withCommitInterval`).                            | Yes                                         |
| Rebalance revokes partitions                                        | Revocation commits the current frontier synchronously; in-flight records for revoked partitions may still complete, and any not-yet-committed work is redelivered to the new assignee.                                          | Possibly (standard at-least-once)           |
| Graceful shutdown                                                   | In-flight records get a bounded drain (default 5s via `Handle.close()`); buffered batch sinks flush; a final synchronous commit runs before the consumer closes. Records that do not finish within the budget stay uncommitted. | Only those that missed the drain            |
| Shutdown timeout exceeded                                           | Remaining workers are interrupted; their offsets stay uncommitted.                                                                                                                                                              | Yes                                         |
| `toMulti` fan-out, one sink throws                                  | **Best effort by design:** the failure is logged and suppressed, other sinks run, the record counts as processed and commits. The DLQ is not invoked. Do not use `toMulti` where every sink must durably receive every record.  | No                                          |
| Unknown topic in `KPipe.multi(...)` (config error / rebalance race) | Dropped at WARNING and the offset **is committed** — a config error must not wedge the consumer into infinite refetch.                                                                                                          | No                                          |
| Interrupt during processing                                         | Interruption is not retried; a coordinated shutdown begins. Offsets of interrupted records are not marked.                                                                                                                      | Yes                                         |

## Ordering

Three distinct things, often conflated:

- **Kafka orders within a partition.** Records with the same key land on the same partition only if your _producer_
  partitions by key (the default partitioner does). KPipe cannot create ordering the topic doesn't have.
- **Processing order** is what `ProcessingMode` controls:
  - `PARALLEL` (default): no ordering. Each record gets its own virtual thread; side effects for offsets 5 and 6 can run
    concurrently or out of order.
  - `SEQUENTIAL`: strictly one record at a time for the whole consumer, in poll order (per-partition offset order,
    partitions interleaved). Use it when any cross-record ordering matters and throughput can afford it.
  - `KEY_ORDERED`: records sharing a key are processed serially in offset order; different keys run in parallel. Key
    equality is byte-equality of the Kafka key; all `null`-keyed records serialize through one queue. The dispatcher
    tracks up to `withKeyOrderedMaxKeys` distinct keys (default 10,000); at the cap, idle queues are evicted, and if
    every tracked key still has work queued, dispatch stalls the consumer thread until one drains (implicit
    backpressure, with a WARNING once per saturation episode).
- **Commit order** is the frontier rule above. It is a durability property, not an execution-ordering property.

So for "Authorize must apply before Capture for the same account": key the _producer_ by account id (so both land on one
partition) **and** run KPipe in `KEY_ORDERED` (or `SEQUENTIAL`). Offset tracking alone does not give you this — in
`PARALLEL` mode the two side effects can interleave even though the commit frontier is correct.

## Backpressure

Enabled by default. When the monitored metric crosses the high watermark (default 10,000), KPipe pauses all assigned
partitions; when it drops to the low watermark (default 7,000), it resumes — hysteresis prevents oscillation. A paused
consumer **keeps polling**: paused partitions fetch nothing, but the poll keeps the consumer's group membership alive
(no `max.poll.interval.ms` eviction) and keeps the pause/resume decision on a fixed cadence. The monitored metric is
in-flight records for `PARALLEL`/`KEY_ORDERED` and consumer lag for `SEQUENTIAL` (where in-flight is always ≤ 1).

During an in-flight hold (`PARALLEL`/`KEY_ORDERED`) the consumer parks for up to the poll timeout and then polls with a
zero timeout; a completion that drains in-flight work to the low watermark ends the park early. The lag-based pause in
`SEQUENTIAL`, a manual pause and a circuit-breaker pause keep the blocking poll. With the classic group protocol, a
zero-timeout poll advances a rebalance one step, where a blocking poll finishes it in one call, so a rebalance that
starts during an in-flight hold takes one or two extra poll timeouts to complete: roughly 100 to 200 ms at the default
100 ms poll timeout, estimated from a probe at a 3 s timeout, where a joining member waited about 6.0 s instead of 2.8
s. The consumer group protocol (`group.protocol=consumer`) runs the rebalance on the broker and is unaffected.

## What actually limits scale

A virtual thread per record removes the worker pool, not the physics. The resources that bound a KPipe consumer in
practice: **partition count** caps fetch parallelism (Kafka delivers a partition's records in order, so more in-flight
work than the topic's partitions can supply never materializes); **heap** grows with in-flight records × payload size,
which is exactly what the backpressure watermark bounds; **pinned virtual threads** — third-party clients that block
inside `synchronized` can pin their carrier and erode the overlap the model relies on (prefer lock-based or
Loom-friendly clients on the hot path); **retry amplification** — `withRetry(n)` multiplies a failing sink's load by up
to n+1; and the usual downstream limits (connection pools, rate limits, file descriptors) arrive sooner than thread
limits do. CPU-bound work gains little from any of this — virtual threads overlap waiting, not computation.

## Sinks

The guarantee boundary is the sink invocation: KPipe guarantees your sink is invoked at least once per non-filtered
record, and that the offset only commits afterward. What the sink does with the record — idempotency, its own
durability, partial writes — is outside KPipe's control. Two documented weakenings: `toMulti` is best-effort per sink
(above), and a batch sink's records are only as durable as the batch flush (the coverage contract and shutdown drain are
described in [SINKS.md](SINKS.md#batch-sinks)).

## External offset stores and managers

With an `OffsetStore` (`Builder.withOffsetStore`), every guarantee above holds unchanged: the default
`KafkaOffsetManager` still computes the frontier and commits it on the same schedule, and the store only replaces where
the offsets are kept. On assignment the consumer seeks to the stored offset, and a failed load is retried rather than
skipped, so a partition never starts from a position the store did not give.

With a custom `OffsetManager`, KPipe still guarantees the _call points_ — track before dispatch, mark only on terminal
success — but the frontier logic itself (lowest-pending, no-commit-ahead) lives in the default `KafkaOffsetManager`. A
custom implementation takes on that responsibility; the invariants it must uphold are specified in
[OFFSET-INVARIANTS.md](OFFSET-INVARIANTS.md). Storing each finished record's offset does not uphold them: in `PARALLEL`
and `KEY_ORDERED` mode a later offset can be stored while an earlier record is still running.

## What this is not

No exactly-once. No Kafka transactions. No guarantee that your side effects are idempotent — at-least-once explicitly
means your sink may see a record twice after a crash, and correctness under duplicates is the application's job.
