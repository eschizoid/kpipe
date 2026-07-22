# KeyOrderedDispatcher v2 — interleaved A/B (2026-07-21)

The standing guardrail for dispatcher-lock work: re-run `KeyOrderedDispatchBenchmark` and demonstrate a
measurable win before landing. This is that record, for the v2 rewrite (single global `ReentrantLock` →
`ConcurrentHashMap` + per-queue monitors).

## Method

Same box as the [2026-07-21 reference capture](2026-07-21.md) (i7-8850H 6c/12t, 16 GB, OpenJDK 25.0.3). Because a
first back-to-back comparison showed the untouched PARALLEL control moving ~35% between runs (afternoon
load/thermal drift), the decisive comparison was run **interleaved** — main-dispatcher → candidate → main →
candidate, four full bench runs at fork=2 — with the PARALLEL arm as the drift canary. Win bar (set before
measuring): ≥15% at the 10,000-key cell with non-overlapping error bars, no regression at the other cells,
all unit + jcstress suites green.

## Result — candidate lands (bar cleared 8×)

Pooled over both rounds (`ops/s`; per-round values were mutually consistent with tight bars):

| cell | v1 (single lock) | v2 (CHM + monitors) | delta |
|---|---:|---:|---|
| KEY_ORDERED, 10,000 keys | 325,321 | **721,582** | **+121.8%** |
| KEY_ORDERED, 100 keys | 348,131 | **739,274** | **+112.4%** |
| KEY_ORDERED, 1 key | 1,202,348 | 1,421,109 | +18.2% |
| PARALLEL (control), 10,000 keys | 1,143,400 | 1,122,845 | −1.8% (canary flat, ±3–4% all four runs) |
| PARALLEL (control), 100 keys | 1,016,118 | 1,227,876 | +20.8%¹ |
| PARALLEL (control), 1 key | 1,101,888 | 1,064,071 | −3.4%¹ |

¹ The k=1/k=100 control cells were noisy (±10–27% in individual runs) and their deltas straddle their own error
bars; the k=10,000 control — the cell that gates the decision — was rock stable. The control shares no code with
the change (PARALLEL uses `ParallelDispatcher`).

KEY_ORDERED at high cardinality moves from ~28% of the PARALLEL ceiling to ~64%. The k=1 improvement is monitor
vs `ReentrantLock` fixed overhead on the uncontended path.

**Scope caveat, stated honestly:** the bench's 10,000-key cell exactly *fills* the default cap but never exceeds
it, so the eviction/saturation path was not perf-measured — in v1 or v2 (both baselines share this). v2 changed
the eviction victim policy (coldest-first LRU → any empty+idle queue); that path's cost is covered by
correctness tests, not by these numbers.

## Correctness evidence

- All dispatcher/consumer unit + property tests green, including two tests added with the rewrite:
  the worker-active eviction guard (an empty-but-active queue must never be evicted) and same-key
  reallocation FIFO after eviction.
- `KeyOrderedWorkerHandoffJCStressTest` (lost/duplicate task under enqueue-vs-worker-exit; formerly
  `KeyOrderedLruJCStressTest`), `KeyOrderedEvictRaceJCStressTest` (cap=1, three actors, evict-vs-redispatch),
  and `KeyOrderedEvictTombstoneJCStressTest` (pre-seeded two-actor variant that collides the tombstone window
  directly and reports window-reachability in its outcome histogram): all configs passed at
  `-iters 3 -time 100 -f 2` — deeper than the CI caps.

Raw JMH JSON: captured under the branch's `.claude/bench/ab-{base,cand}-r{1,2}.json` on the capture machine
(dev-loop artifacts, not committed; the numbers above are the pooled scores).
