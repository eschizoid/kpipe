# Batch flush lock scope — dispatch moved out of the lock (2026-09-16)

The standing rule for lock work in this project is a measured win before landing. This is that record, for #335: moving
`BatchPipelineWrapper`'s per-record outcome dispatch out of the per-topic flush lock while leaving `sink.apply` under
it.

## Win bar, fixed before anything was measured

- Gating cell: the longest lock hold — a whole-batch failure against a DLQ that will not ack promptly.
- Bar: a large, unambiguous reduction in how long a competing `enqueue` on the same topic blocks.
- No regression in the consumer suite, in particular the offset ordering and invariant property tests, since letting
  dispatches overlap is the one real semantic change.
- If the bar is not cleared, #335 closes with the measurement and no code change.

## Method — and why this is not a JMH benchmark

The first attempt was a JMH throughput benchmark driving the public facade through `MockConsumer` with a failing sink
and a DLQ producer that parks per send. It was abandoned, and the reason is worth recording because the same trap is
waiting for the next person.

End-to-end throughput is a distant proxy for a lock-hold effect. Two harness defects had to be fixed before the numbers
meant anything at all — `MockProducer.send` is `synchronized`, so parking inside an overriding `synchronized` method
serialized every DLQ send on the producer's own monitor and made both arms identical for a reason unrelated to the
change; and a `Thread.onSpinWait` wait loop pinned a carrier thread away from the virtual threads doing the work. Even
with both fixed, the gating cell returned `483 ± 185 ops/s` — a 38% relative error against a bar of 25%. Error bars
wider than the effect are not evidence of absence, they are absence of evidence, so the instrument was dropped rather
than shipped.

What replaced it measures the claim directly. `BatchFlushLockHoldTest` constructs the wrapper in-package (no reflection,
no widened visibility), fills a batch on one thread against a sink that always throws, and times how long a competing
`enqueue` on a second thread blocks while the resulting whole-batch failure dispatches. The callback parks 10ms per
record, standing in for a broker ack.

## Result — bar cleared

| arm                                   | competing `enqueue` blocked |
| ------------------------------------- | --------------------------: |
| dispatch inside the lock (before)     |                  **221 ms** |
| dispatch outside the lock (candidate) |                    **0 ms** |

Dispatch budget in both arms: 20 records × 10 ms = 200 ms. The before-arm blocks for the entire dispatch and then some,
because the competitor also waits out the sink call ahead of it.

The 10ms stand-in is what makes this a lower bound rather than the real cost. In production the per-record wait is
bounded by `max.block.ms` plus `delivery.timeout.ms`, which add; at the 120s default and a 500-record batch the hold is
on the order of hours, during which every worker enqueueing to that topic blocks and `close()` contends on the same
lock.

## Regression guard

The measurement is the test. Reverting the production change and re-running it reports 166 ms blocked and fails the
assertion, so the guard discriminates the change rather than merely passing alongside it.

## Suite

429 tests in `:lib:kpipe-consumer`, green. Four integration tests (`ChaosRebalance`, `ExternalOffset`,
`CrashRestartReprocessing`, `KPipeProducer`) fail locally with `DockerClientProviderStrategy` errors — no Docker on this
box — and fail identically on `main`, so they are environmental. The offset ordering and invariant property tests pass,
which is the result that matters here: dispatches may now overlap, and nothing downstream depends on them not doing so.
