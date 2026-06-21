# Offset-lifecycle invariants

This is the checkable contract for `KafkaOffsetManager` that the correctness harness verifies against. Each invariant is
phrased as a property a test can assert.

## Toolchain status

- **jqwik** (Java-native property testing) is wired and green. `OffsetInvariantPropertyTest` generates randomized
  track/mark sequences and asserts I1 on a single partition. This is the active sequence-property layer.
- **Lincheck** (concurrency model-checking) was attempted but does not run on JDK 25 yet. Lincheck 2.39 (the newest
  release) bundles an ASM that rejects class-file major version 69 (Java 25); its runtime bytecode-instrumentation pass
  throws `Unsupported class file major version 69` while retransforming classpath classes, which crashes the test JVM.
  Kotlin 2.2.0 also cannot emit JVM target 25 bytecode (tops out at 24). The Lincheck layer is deferred until a release
  supports JDK 25; the invariants below are written tool-agnostically so it can be flipped on later without rework.

## Vocabulary

- **track**: `trackOffset(record)` — adds `record.offset()` to the per-partition pending set
  (`ConcurrentSkipListSet<Long>`). Marks "this offset has started processing."
- **mark**: `markOffsetProcessed(record)` — removes `record.offset()` from the pending set and bumps the per-partition
  `highestProcessedOffset`. Marks "this offset reached a terminal state."
- **commit point**: the offset the manager would commit to Kafka right now for a partition. In Kafka's "next offset"
  model this is the offset of the next record expected, so a partition that has fully processed offset `N` commits
  `N + 1`. Observable per-partition through `getPartitionState(partition).get("nextOffsetToCommit")`:
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

## I3 — no loss (commit advances only over contiguous completed offsets)

Every tracked offset eventually reaches a terminal state (it is either still pending or has been marked), and the commit
point advances only over a contiguous run of completed offsets — it never skips an un-marked offset.

**Assertable property.** Start from tracked set `T` and marked set `M ⊆ T`. The committed offset never exceeds
`min(T \ M)` (the lowest still-unprocessed offset) when `T \ M` is non-empty. When `T \ M` is empty (everything tracked
has been marked), the commit point is `max(M) + 1`. There is no interleaving that commits an offset whose record was
tracked but not marked.

## I4 — revocation (commit-and-clear on revoke)

On `onPartitionsRevoked`, for each revoked partition the manager commits the current commit point (lowest pending, else
`highestProcessed + 1`), then clears that partition's pending set, highest-processed entry, and cached `TopicPartition`
before the partition can be reassigned. Stale queued commit commands that reference revoked partitions are dropped from
the command queue.

**Assertable property.** After `onPartitionsRevoked([p])`:

- `kafkaConsumer.commitSync(...)` was invoked with an entry for `p` equal to the pre-revoke commit point (when `p` had
  any tracked/processed state);
- `getPartitionState(p)` reports `nextOffsetToCommit == -1` and `pendingCount == 0` (state cleared);
- any `CommitOffsets` command still in the queue no longer contains `p`.

## I5 — rebalance-safe (mark after revoke is a clean no-op)

`markOffsetProcessed` (or `trackOffset`) for a partition that was already revoked/cleared is a clean no-op: no
exception, and it does not resurrect cleared partition state in a way that produces a bogus commit point.

**Assertable property.** Given a partition whose state was cleared by `onPartitionsRevoked`, a subsequent
`markOffsetProcessed` for that partition completes without throwing. (Note: because the manager keys state by
`TopicPartition` and re-creates entries lazily, a post-revoke mark may re-create a `highestProcessedOffset` entry; the
invariant the harness checks is the *no-exception, no-corruption* property — a re-created entry still obeys I1–I3 for
whatever is tracked after the revoke. It must never produce a commit point that skips a still-pending offset.)
