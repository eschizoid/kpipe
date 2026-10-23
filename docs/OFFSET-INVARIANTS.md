# Offset-lifecycle invariants

This is the checkable contract for `KafkaOffsetManager` that the correctness harness verifies against. Each invariant is
phrased as a property a test can assert.

## Toolchain status

- **jqwik** (Java-native property testing) is wired and green. `OffsetInvariantPropertyTest` generates randomized
  track/mark sequences and asserts I1 on a single partition. `OffsetOrderingPropertyTest` extends this to several
  partitions and to sequences that interleave tracks and marks with revokes, losses and assignments, checking I1, I2,
  I4's commit-and-clear and I5 against an independent model after every step, including that a partition this member no
  longer owns never has a commit point. This is the active sequence-property layer.
- **Fray** (CMU PASTA controlled-concurrency checker) is wired and green on JDK 25 via a per-module `frayTest` source
  set, and is the CI concurrency gate. It explores thread schedules under its own scheduler and replays a failing one
  deterministically, rather than running racy code many times and hoping the bad interleaving appears.
  `OffsetManagerFrayTest` and `RemoveIfEmptyFrayTest` cover the track/mark/revoke and remove-if-empty interleavings —
  the layer the single-threaded property tests cannot reach. The gate is falsification-tested: splitting the atomic
  remove-if-empty into a separate check and removal makes `RemoveIfEmptyFrayTest` fail on its first iteration, naming
  the lost offset. This fills the role Lincheck would have, and unlike Lincheck it runs on JDK 25 (below).

  Fray controls scheduling but does not model hardware reordering, so visibility bugs are outside its reach. The
  jcstress suite it replaced did not cover that either in practice: its one memory-model test forbids only an outcome
  that x86 cannot produce, and CI has only ever run x86.

- **Toxiproxy** sits between a Testcontainers broker and the consumer in `BrokerFaultInjectionIntegrationTest`, which
  checks I1 to I3 end to end while the broker is unreachable, drops responses, answers slowly or resets connections.
  Every commit the consumer has acknowledged is checked through a `ConsumerInterceptor`, and a sampler reads the
  committed offsets on the broker's own listener, which also catches a commit that landed but whose response was
  dropped; neither may pass a record that has not completed. In `PARALLEL` and `KEY_ORDERED`, one record per partition
  is released while the fault is in effect, so the frontier has to advance while commits cannot get through, and another
  only after the link has recovered, so the resumed commits must stop below it. `SEQUENTIAL` runs a record on the
  consumer thread, where holding one would stop commits altogether, so in that mode the fault scenarios check only that
  commits resume and nothing is lost. The test also closes a consumer while the broker is unreachable, with the broker
  returning after or during the close, and requires the next consumer to receive every record the first left unfinished.
  Making the frontier skip pending offsets fails every `PARALLEL` and `KEY_ORDERED` case, and treating an interruption
  at shutdown as a failure fails the cases where the broker returns during the close, in every mode, and the
  `SEQUENTIAL` case where it returns after: a consumer thread `close()` had to interrupt is still tearing down when the
  broker comes back, so its commit lands.

- **A three-broker cluster** runs in `MultiBrokerFaultIntegrationTest`, with every partition replicated on all three
  brokers and two in-sync replicas required, as `__consumer_offsets` requires them. A fault pauses broker containers
  mid-run: the leader of every partition, a follower, the group coordinator, or both followers. The same two commit
  checks run throughout, in every mode, with records held across the fault. With both followers paused the brokers
  refuse every commit, so none may be acknowledged; with one broker paused, commits must resume on the others before the
  fault ends. How soon they resume after losing the group coordinator depends on the client's `request.timeout.ms`: the
  client looks for a new coordinator only once its request to the silent one times out, 30 seconds by default, and until
  then commits stall without losing anything. The test sets it to five seconds.

- **Lincheck** (concurrency model-checking) was attempted but does not run on JDK 25 yet. Lincheck 2.39 (the newest
  release) bundles an ASM that rejects class-file major version 69 (Java 25); its runtime bytecode-instrumentation pass
  throws `Unsupported class file major version 69` while retransforming classpath classes, which crashes the test JVM.
  Kotlin 2.2.0 also cannot emit JVM target 25 bytecode (tops out at 24). The Lincheck layer is deferred until a release
  supports JDK 25; the invariants below are written tool-agnostically so it can be flipped on later without rework.

## Vocabulary

- **track**: `trackOffset(record)` — adds `record.offset()` to the per-partition pending set (`PendingOffsetSet`, a
  monitor-synchronized sorted primitive-`long` window; previously `ConcurrentSkipListSet<Long>`, replaced to eliminate
  per-record boxing and skiplist-node churn). Marks "this offset has started processing."
- **mark**: `markOffsetProcessed(record)` — removes `record.offset()` from the pending set and bumps the per-partition
  `highestProcessedOffset`. Marks "this offset reached a terminal state." A mark counts only for an offset that is still
  pending; marking any other offset changes nothing.
- **commit point**: the offset the manager would commit to Kafka right now for a partition. In Kafka's "next offset"
  model this is the offset of the next record expected, so a partition that has fully processed offset `N` commits
  `N + 1`. Observable per-partition through `getPartitionState(partition).nextOffsetToCommit()`:
  - if any offset is still pending → the lowest pending offset (commit cannot pass it);
  - else if anything was processed → `highestProcessedOffset + 1`;
  - else → `-1` (nothing tracked).

The commit point is the single value every invariant below constrains.

## I1 — lowest-pending (no commit past a gap)

The commit point for a partition never advances past a gap. If offsets `100` and `102` are tracked and only `102` is
marked processed, the commit point stays at `100` (because `101` is still pending), never `103`.

**Assertable property.** For any interleaving of track/mark on a single partition, let `P` be the set of pending
(tracked but not yet marked) offsets and `H` the highest marked offset. Then:

- if `P` is non-empty, `nextOffsetToCommit == min(P)`;
- the commit point is always `<= min(P)` when `P` is non-empty — it can never reach `min(P) + 1` or beyond while
  `min(P)` is pending.

The jqwik smoke test asserts exactly this: over a random sequence of track/mark ops, after every step the commit point
is `<= ` every still-pending offset.

## I2 — no commit-ahead (terminal-before-eligible)

An offset is eligible to contribute to the commit point only once its record is terminal (processed/marked). A tracked
but unmarked offset is never treated as committed.

**Assertable property.** An offset `N` that has been tracked but not marked is always a member of the pending set; the
commit point for its partition is `<= N`. Equivalently: `highestProcessedOffset + 1` can become the commit point only
when the pending set is empty (no unmarked offset is holding the line lower).

**Shutdown-tail tolerance.** This is a `<=` invariant, not `==`. At a bounded graceful-close shutdown the commit point
may legitimately trail the log end by a processed-but-not-yet-committed tail — those records are simply reprocessed on
restart, which is the definition of at-least-once tolerance. Requiring the commit to reach exactly the log end at
shutdown would be an exactly-once / complete-drain property kpipe does not claim. End-to-end tests therefore assert
no-loss (every record observed) plus this `<=` bound, never `committed == logEnd` after a close. A running consumer's
periodic commits reaching the log end once every record has completed is a different property, liveness, and a test may
assert it before closing.

## I3 — no loss (commit advances only over contiguous completed offsets)

Every tracked offset eventually reaches a terminal state (it is either still pending or has been marked), and the commit
point advances only over a contiguous run of completed offsets — it never skips an un-marked offset.

**Assertable property.** Start from tracked set `T` and marked set `M ⊆ T`. The committed offset never exceeds
`min(T \ M)` (the lowest still-unprocessed offset) when `T \ M` is non-empty. When `T \ M` is empty (everything tracked
has been marked), the commit point is `max(M) + 1`. There is no interleaving that commits an offset whose record was
tracked but not marked.

**Durable-terminal-state rule (DLQ failure).** A record is marked (offset eligible to advance) only once it reaches a
durable terminal state: either the sink processed it, or it was safely parked in the dead-letter topic. When a record
fails processing AND the DLQ send itself fails (broker down, producer closed, oversized/serialization error), it is in
neither place, so the offset is NOT marked — it stays pending and is re-fetched (and the DLQ retried) on the next
restart or partition reassignment. (The commit point holds, but the live fetch position has already advanced, so the
record is not re-delivered within the running session — only after the partition seeks back to the committed offset on
restart/rebalance.) A down DLQ therefore stalls the commit point rather than silently dropping data — a commit stall,
not backpressure: the consumer is not paused and the fetch position keeps advancing in-memory; only the committed offset
is pinned. The failure is observable through the `dlqFailed` counter and an ERROR log. This holds on both the per-record
path (`handleProcessingError`) and the batch path (`BatchCallbacks.onBatchFailure`). The exception is a consumer with no
DLQ configured: there the caller has explicitly opted into log-and-advance, so a processing failure marks the offset and
moves on.

## I4 — revocation (commit-and-clear on revoke)

On `onPartitionsRevoked`, for each revoked partition the manager commits the current commit point (lowest pending, else
`highestProcessed + 1`), then clears that partition's pending set, highest-processed entry, and cached `TopicPartition`
before the partition can be reassigned. A commit request already submitted through the `CommitExecutor` seam but not yet
executed cannot commit a revoked partition: its offsets supplier reads the ledger on the consumer thread at execution
time — after this callback has cleared the revoked partitions — so revoked entries are structurally invisible to it (no
queue rewriting involved).

**Assertable property.** After `onPartitionsRevoked([p])`:

- `kafkaConsumer.commitSync(...)` was invoked with an entry for `p` equal to the pre-revoke commit point (when `p` had
  any tracked/processed state);
- `getPartitionState(p)` reports `nextOffsetToCommit == -1` and `pendingCount == 0` (state cleared);
- any still-pending `CommitOffsets` request, when its offsets supplier is invoked, computes a map with no entry for `p`.

## I5 — rebalance-safe (mark after revoke is a clean no-op)

`markOffsetProcessed` for a partition that was already revoked/cleared is a clean no-op: no exception, and no partition
state comes back. A mark counts only for an offset that is still pending, so it cannot recreate a cleared partition.
`trackOffset` after a revoke does start a new pending window; that is how a reassigned partition resumes.

**Assertable property.** Given a partition whose state was cleared by `onPartitionsRevoked`, a subsequent
`markOffsetProcessed` for that partition completes without throwing and leaves the partition with no commit point
(`nextOffsetToCommit == -1`). A record that finishes after its partition was revoked therefore cannot recreate the
partition, and the member does not commit an offset for it again.
