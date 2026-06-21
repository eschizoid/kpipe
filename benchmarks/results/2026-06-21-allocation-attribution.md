# Allocation attribution — 2026-06-21

Follow-up to [`2026-06-20.md`](2026-06-20.md), which flagged KPipe (PARALLEL) at **1,628 B/op** — the highest per-record
allocation in the competitive suite (vs Confluent PC's 33). This note attributes that number to specific paths so the
optimization effort targets the right thing instead of guessing.

## Method

`gc.alloc.rate.norm` (bytes allocated per op) is **deterministic** — it's a property of the code path, not of timing —
so it is trustworthy even on a noisy local machine where throughput is not. This run's throughput error bars were
enormous (±150%); the allocation error bars were tiny (OffsetManager ±0.7 B/op). Read only the allocation column here.

Two of the three hot-path components were measured **in isolation** with the Docker-free benches:

```bash
./gradlew :benchmarks:jmh \
  -Pjmh.includes='OffsetManagerBoxing|KeyOrderedDispatch' \
  -Pjmh.iterations=3 -Pjmh.warmupIterations=2 -Pjmh.fork=1 -Pjmh.profilers='gc'
```

## Attribution of the 1,628 B/op (PARALLEL)

| Path (measured in isolation)                                         |   B/op | Share |
| -------------------------------------------------------------------- | -----: | ----: |
| Dispatcher + virtual-thread creation (`KeyOrderedDispatch` PARALLEL) | ~1,010 |  ~62% |
| Offset track + mark (`OffsetManagerBoxing`, ±0.7)                    |    396 |  ~24% |
| Pipeline (deserialize + `Result` + operators), by difference         |   ~220 |  ~14% |

## Conclusions

1. **`Result.Passed` is not the target.** It is ~24 B/op of 1,628 — noise. Do not micro-optimize it. (The original hunch
   that it was "the one number to chase" was wrong; profiling is why we know.)

2. **Virtual-thread creation (~62%) is the cost of the model, not a defect.** A fresh virtual thread per record
   allocates the `VirtualThread` + continuation + initial stack chunk. This is the _same_ property that lets KPipe
   absorb blocking work and beat the platform-thread-pool runtimes at `workMicros ≥ 100` — and it is exactly why
   Confluent PC, which reuses a fixed worker pool, allocates almost nothing. Cutting it means pooling virtual threads,
   which is a Loom anti-pattern and would surrender the blocking-work win. **It is documented as the price of
   VT-per-record, not chased.**

3. **The only genuinely addressable lever is the offset manager's 396 B/op** — `Long` boxing into the per-partition
   `ConcurrentSkipListSet` plus skiplist-node churn (the work deferred from the post-#170 audit; #205 only cached
   `TopicPartition`). A primitive-long pending-offset structure could reclaim a meaningful fraction.

## Sequencing decision

The 396 B/op lives in the **concurrency-critical** structure that _is_ the lowest-pending-offset at-least-once
invariant. Replacing `ConcurrentSkipListSet<Long>` with a hand-rolled primitive sorted set introduces concurrency risk
in the exact component slated for correctness verification next. So the optimization is **deferred into the correctness
phase**: build the offset-lifecycle spec + Lincheck harness first, then optimize the pending-offset structure _on top of
that safety net_ — proving the faster version preserves the invariants by the harness we'll have just built. Optimizing
before verifying would be backwards.

**Net:** no large standalone allocation win is available today without either abandoning VT-per-record (no) or touching
the pre-verification concurrency-critical structure (deferred, by design). The attribution is the deliverable; the cut
rides the correctness phase.
