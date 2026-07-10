# KPipe Benchmark Methodology

This document is how to read the numbers in `benchmarks/results/` and how to reproduce them. If you're tempted to quote
a single number from a single run, read this first.

## What the suite measures

| Bench                                | Question it answers                                                                                                                                                                                         |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `JsonPipelineBenchmark`              | What does KPipe's single-SerDe-cycle save vs naive byte-to-byte chaining? (Design validation, not competitive.)                                                                                             |
| `AvroPipelineBenchmark`              | What does zero-copy magic-byte handling save vs `Arrays.copyOfRange`? (Design validation.)                                                                                                                  |
| `ParallelProcessingBenchmark`        | How does **throughput** compare across KPipe (PARALLEL + KEY_ORDERED), the Kafka Share Consumer (KIP-932), Confluent Parallel Consumer, Reactor Kafka, and a hand-rolled `KafkaConsumer + virtual threads`? |
| `ParallelProcessingLatencyBenchmark` | How do KPipe, Confluent PC, Reactor, and raw `KafkaConsumer + VT` compare on **per-batch latency** (p50, p95, p99)?                                                                                         |
| `PerRecordLatencyHarness`            | What is the true **per-record latency distribution** (p50/p95/p99/max) across KPipe PARALLEL, KPipe KEY_ORDERED, Confluent PC UNORDERED, and raw `KafkaConsumer + VT`? (Not JMH — see below.)               |
| `BatchSinkLatencyBenchmark`          | What does `Stream.toBatch(...)` save when the destination has nontrivial per-call cost?                                                                                                                     |

The first two are KPipe-vs-straw-man. They show the design choices paid off but are not competitive claims. Use them in
design docs, not in pitches.

The next two are the competitive suite. **These are the ones to quote** when comparing KPipe to the alternatives a JVM
team would actually pick.

The last one is KPipe alone, parameterised across batch size and sink latency.

## What the parallel runtimes look like

| Runtime                                     | Concurrency primitive                                                                 | Ordering          | Configured concurrency                                                                |
| ------------------------------------------- | ------------------------------------------------------------------------------------- | ----------------- | ------------------------------------------------------------------------------------- |
| **Single-threaded `KafkaConsumer`**         | One platform thread, `poll()` + inline processing, no fan-out                         | **per-partition** | 1 thread (the floor; the default most teams ship with)                                |
| **KPipe (PARALLEL)**                        | Virtual thread per record (Loom)                                                      | none              | Unbounded (virtual-thread-per-record); the consumer's in-flight watermark caps memory |
| **KPipe (KEY_ORDERED)**                     | Per-key serial queues, each drained by a virtual thread                               | **per-key**       | Unbounded across keys; serial within a key                                            |
| **Kafka Share Consumer (KIP-932)**          | One `KafkaShareConsumer` + `newVirtualThreadPerTaskExecutor`, per-poll-batch barrier  | none              | Unbounded virtual threads; barrier bounds it to one in-flight batch                   |
| **Confluent Parallel Consumer (UNORDERED)** | Platform-thread worker pool, `ProcessingOrder.UNORDERED`                              | none              | `maxConcurrency=100`                                                                  |
| **Confluent Parallel Consumer (KEY)**       | Same pool, `ProcessingOrder.KEY`                                                      | **per-key**       | `maxConcurrency=100`; at most one in-flight per key                                   |
| **Confluent Parallel Consumer (PARTITION)** | Same pool, `ProcessingOrder.PARTITION`                                                | **per-partition** | `maxConcurrency=100`; capped at partition count (8)                                   |
| **Reactor Kafka**                           | Reactor `parallel` scheduler via `Flux.parallel(N)`                                   | none              | `parallel(100)` to match Confluent's pool size                                        |
| **Raw `KafkaConsumer` + virtual threads**   | `Executors.newVirtualThreadPerTaskExecutor()` driven from a platform-thread poll loop | none              | Unbounded virtual threads                                                             |

> **Confluent Parallel Consumer dependency note.** As of 2026-05, `confluentinc/parallel-consumer` is officially no
> longer maintained ("Update README with maintenance notice", commit on `master` 2026-05-21) and points to
> [`astubbs/parallel-consumer`](https://github.com/astubbs/parallel-consumer) as the active fork. That fork has set up
> Maven Central publishing infrastructure (commit "feat(publish): Maven Central publishing", 2026-04-22) but **has not
> yet released a stable artifact** — the source pom is `0.6.0.0-SNAPSHOT` and `io.github.astubbs.parallelconsumer`
> returns zero results on Maven Central. The benchmarks therefore still depend on the deprecated
> `io.confluent.parallelconsumer:parallel-consumer-core:0.5.3.3`. Swap when Stubbs publishes a release.

Two axes worth attention. **Threading model**: Loom-based (KPipe PARALLEL/KEY_ORDERED, share, raw) vs platform-threaded
(single-threaded, Confluent's three modes, Reactor) — Loom absorbs blocking work; platform pools don't. **Ordering
coordination**: light (UNORDERED, KEY across many keys) vs heavy (CPC PARTITION's per-partition locks); the gap between
CPC KEY and CPC PARTITION makes this visible even though both have the same 100-worker pool. At `workMicros=0` the suite
measures framework overhead; at `workMicros=1000` it measures blocking-work scheduling. Those two questions often have
different winners.

### Apples-to-apples — read these comparisons honestly

The share consumer and the key-ordered dispatcher do **not** solve the same problem, so the suite pins every variable
except the consumption mechanism (same broker, same seeded data, same `workMicros`, same `@OperationsPerInvocation`
throughput metric, work fanned to virtual threads everywhere) and you read each pairing for what it actually isolates:

- **KPipe PARALLEL vs Share Consumer** is the fair head-to-head: both unordered, both at-least-once, both one process
  with virtual-thread fan-out. The only difference is the fetch+durability protocol — KPipe's fetch + lowest-pending
  offset commit vs the share-acquire + per-record acknowledgement path. The share arm's per-poll-batch barrier (process
  the batch, then poll again, which implicitly ACCEPTs it) is the honest cost of processing a share batch concurrently
  while staying at-least-once, not a handicap.
- **KPipe KEY_ORDERED vs Confluent PC KEY** is the cleanest cross-library "what does per-key ordering cost?" comparison:
  same guarantee (per-key FIFO), different implementations (KPipe's per-key VT-per-queue vs CPC's platform-thread pool
  with key gating). Read the gap as an implementation cost, not a guarantee difference.
- **KEY_ORDERED is the ordering tax, not a rival.** It is the only arm that guarantees per-key order; no other runtime
  here (share included) can provide ordering at all. Read its gap below the unordered arms as "what ordering costs,"
  never as "X beats KEY_ORDERED."
- **Single process, not scale-out.** Every arm is one consuming process. This deliberately does **not** test the share
  consumer's headline feature — members beyond partition count — which would need a multi-instance share group. That's a
  separate experiment; the numbers here say nothing about it.

## JMH configuration

The Gradle JMH plugin is configured in `benchmarks/build.gradle.kts`. Defaults:

| Parameter              | Default              | Override                   |
| ---------------------- | -------------------- | -------------------------- |
| Warmup iterations      | 3                    | `-Pjmh.warmupIterations=N` |
| Measurement iterations | 5                    | `-Pjmh.iterations=N`       |
| Forks                  | 1                    | `-Pjmh.fork=N`             |
| Threads                | 1                    | `-Pjmh.threads=N`          |
| Benchmark mode         | `thrpt`              | n/a (per-benchmark)        |
| Time unit              | `s`                  | n/a                        |
| Failure handling       | `failOnError = true` | n/a                        |
| Result format          | `TEXT`               | `-Pjmh.resultFormat=JSON`  |

Per-benchmark overrides:

- `ParallelProcessingLatencyBenchmark` sets `Mode.SampleTime + Mode.AverageTime` and `OutputTimeUnit.MILLISECONDS` to
  publish percentile histograms.
- `BatchSinkLatencyBenchmark` parameterises over `batchSize × sinkLatencyMicros`.
- `ParallelProcessingBenchmark` parameterises over `workMicros` (`0 / 100 / 1000 / 10000 / 35000 / 50000 / 100000`). The
  bottom three values isolate framework overhead through 1ms blocking work; the upper four cover the 10-100ms regime
  that most real Kafka consumers actually live in (HTTP/DB hops) and the regime where lag-based backpressure becomes
  load-bearing.

### Recommended publishing run

This is the full publishing capture: every arm, each benchmark's full `workMicros` sweep (no override — including the
10–100ms regime where most real consumers live), latency percentiles included, on a quiesced box (no browser, no IDE
indexing, mains power). Plan for a weekend, not an overnight: the single-threaded arm is the long pole — 25k records ×
100ms ≈ 42 min per iteration at the top cell, which at fork=5 is a multi-day tail (and why this run cannot fit CI's
6-hour cap). To fit an actual overnight window, exclude the serial arm's pathological cells with the include-regex trick
from the CI section and capture `singleThread` separately at lower fork counts.

```bash
./gradlew :benchmarks:jmh \
  -Pjmh.includes='ParallelProcessingBenchmark|ParallelProcessingLatencyBenchmark' \
  -Pjmh.warmupIterations=3 \
  -Pjmh.iterations=5 \
  -Pjmh.fork=5 \
  -Pjmh.profilers='gc' \
  -Pjmh.resultFormat=JSON
```

Five forks, not two: the 2026-06-20 CI capture at fork=2 produced 25%+ error bars and two cells where JMH could not
compute a stable cross-fork stddev at all (`NaN` score-error) — fork=2 is the floor for _running_, not for _publishing_.
The `gc` profiler adds allocation rate and GC count per benchmark — required input to the "throughput vs
allocation-cost" trade-off. Afterwards, copy `build/results/jmh/results.json` into `results/` alongside a dated markdown
snapshot per [What to write down with every published number](#what-to-write-down-with-every-published-number).

### Quick smoke run (for local sanity-check)

```bash
./gradlew :benchmarks:jmh \
  -Pjmh.includes='ParallelProcessingBenchmark\.kpipe$' \
  -Pjmh.iterations=1 \
  -Pjmh.warmupIterations=1 \
  -Pjmh.fork=1
```

One iteration, one warmup, one fork, only the KPipe arm. Useful for verifying the harness didn't break after changes;
meaningless as a measurement. Note: `-Pjmh.includes` is a regex matched against the benchmark FQN — `@Param` values
(like `workMicros`) are **not** part of the FQN and can't be filtered through this property, so the smoke run exercises
all configured `workMicros` cells of the included arm. Swap `kpipe` for any other arm name (`share`, `kpipeKeyOrdered`,
`singleThread`, `confluentKey`, etc.) to spot-check that one.

### Running in GitHub CI (what fits, what doesn't)

The Benchmarks workflow runs on a shared 4-vCPU runner — read the **ordering**, never the absolutes (see
[`results/2026-06-20.md`](results/2026-06-20.md) for the first CI capture). Two things bound what's runnable:

1. **`workMicros` is now overridable** via `-Pjmh.workMicros=<csv>` (wired to JMH `benchmarkParameters`). Empty = the
   benchmark's full `@Param` sweep (an overnight run); pass a subset for CI.
2. **The serial arms don't fit a high-`workMicros` run.** `singleThread` is serial (25k records × 100ms ≈ 42 min /
   iteration at `workMicros=100000`) and `confluentPartition` is capped at the partition count (8-way), so both blow
   past GitHub's 6-hour job cap in the `10ms–100ms` regime. Exclude them and the parallel arms fit.

**Low regime (the publishable cells — fits CI easily):**

```bash
./gradlew :benchmarks:jmh \
  -Pjmh.includes='ParallelProcessing' \
  -Pjmh.fork=2 -Pjmh.profilers='gc' -Pjmh.resultFormat=JSON \
  -Pjmh.workMicros='0,100,1000'
```

**High regime (`10ms–100ms`, the I/O-hop range — fits CI only with the serial arms excluded):**

```bash
# Exclude singleThread + confluentPartition; the `$` anchors keep `confluent` from also matching
# `confluentPartition` (JMH includes are substring regexes over the FQN, method name last).
./gradlew :benchmarks:jmh \
  -Pjmh.includes='ParallelProcessingBenchmark\.(kpipe$|kpipeKeyOrdered$|confluent$|confluentKey$|reactor$|raw$|share$)' \
  -Pjmh.fork=2 -Pjmh.profilers='gc' -Pjmh.resultFormat=JSON \
  -Pjmh.workMicros='10000,35000,50000,100000'
```

The complete picture — every arm including `singleThread` across the full sweep — stays a **local overnight run** on a
quiesced box. CI gives the competitive ordering for the cells that fit; it does not replace the reference local run.

### Latency percentiles

`ParallelProcessingLatencyBenchmark` declares `@BenchmarkMode({SampleTime, AverageTime})`. The Gradle config does
**not** set a global `benchmarkMode` (doing so would override that annotation and force throughput mode — which silently
happened in the first CI capture, producing no percentiles). To capture p50/p95/p99, just run the latency bench; its
annotation drives the mode:

```bash
./gradlew :benchmarks:jmh \
  -Pjmh.includes='ParallelProcessingLatencyBenchmark' \
  -Pjmh.fork=2 -Pjmh.resultFormat=JSON \
  -Pjmh.workMicros='100'
```

(fork=2 here is the quick spot-check; percentiles you intend to publish belong in the fork=5 publishing run above.)

### Per-record latency harness (true tail latency)

The JMH latency bench above divides whole-batch completion time by `@OperationsPerInvocation` — a batch-amortized
figure, not a per-record distribution (see the methodological note in `results/2026-07-09.md`). For honest per-record
p50/p95/p99/max, use `PerRecordLatencyHarness` — a plain `main()` harness, not JMH, because JMH cannot ingest tens of
thousands of externally-measured timestamp pairs into its `SampleTime` histogram:

```bash
./gradlew :benchmarks:perRecordLatency \
  -Platency.workMicros=100 \
  -Platency.ratePerSecond=2000 \
  -Platency.warmupRecords=5000 \
  -Platency.measuredRecords=20000 \
  -Platency.arms=kpipeParallel,kpipeKeyOrdered,confluentUnordered,rawVirtualThreads
```

All properties are optional (the values above are the defaults). Docker is required — the harness reuses the suite's
Testcontainers broker. Results print as a table and land as JSON in
`benchmarks/build/results/per-record-latency/results.json` (`-Platency.output=<path>` to override).

How it works: the producer stamps each record's payload with its **intended** `System.nanoTime()` send slot (the
schedule slot, not the actual send time — the coordinated-omission correction, so producer stalls surface as latency
instead of being excused); the sink computes `now − stamp` after the simulated per-record work. Every sample is retained
in a `long[]` and sorted once — exact percentiles, no histogram binning error.

Reading the numbers:

- **Clock caveat — producer and consumer run in one JVM by design.** `System.nanoTime()` origins are per-process, so
  `now − stamp` is only valid because both ends share a process. Do not split the harness across processes and keep this
  scheme, and do not compare its absolute values against distributed-tracing latencies.
- **The measured quantity is intended-produce → work-complete**: producer send (`linger.ms=0`), broker hop over Docker
  loopback, consumer poll, framework dispatch, and the simulated work. Compare arms against each other; the absolute
  values are not production SLA numbers.
- **Keep the rate below every arm's saturation throughput** (defaults: 2,000 rec/s at 100 µs work — far below all
  measured ceilings). Above saturation, percentiles measure backlog depth, not the framework. Sanity-check any raised
  rate/work against the throughput suite first.
- Warmup records (group join, first poll, JIT) are excluded from the distribution via a phase marker in the payload.

## What to write down with every published number

A bench number without context is unverifiable. Each entry in `results/` should record:

1. **Date** — when the run completed.
2. **Hardware** — CPU model, core count, RAM. (Apple Silicon M2 Pro 10c/16GB, AWS m7i.4xlarge, GitHub Actions Linux
   runner spec, etc.)
3. **OS + kernel** — affects scheduler behaviour, especially for Loom.
4. **JDK build** — `java -version`. Loom semantics changed across 21 → 25.
5. **JMH config** — iterations / warmup / forks / profilers actually used.
6. **Benchmark scope** — which classes and which `@Param` cells.
7. **Result file** — the raw `results.json` or `results.text` from the run, committed alongside the summary.

## Reading the numbers

### Throughput mode (`ops/s`)

The `ParallelProcessingBenchmark` uses `@OperationsPerInvocation(TARGET_MESSAGES)` where `TARGET_MESSAGES = 25_000`. So
the JMH "ops/s" number is **records processed per second**.

Don't multiply by `TARGET_MESSAGES`. The `@OperationsPerInvocation` annotation already normalises.

### Sample-time mode (latency percentiles)

`ParallelProcessingLatencyBenchmark` reports per-op time in milliseconds. Same `@OperationsPerInvocation` so an "op" is
one record — but the sampled duration is the whole 25k-record drain, so its "percentiles" are **batch completion time
amortized per record**, not a per-record tail (the 2026-07-09 capture documents this). Do not quote them as "p99
latency"; for that, use the [per-record latency harness](#per-record-latency-harness-true-tail-latency). For true
per-record distributions:

- **p50** — typical record. Lower is better.
- **p95 / p99** — tail. This is what matters when SLA-bound consumers are involved.
- **p100 / max** — the worst observation in the run. Highly noisy; don't quote.

A runtime can win on average throughput while losing on p99 — `Flux.parallel(N)` schedules records in a fan-out / fan-in
pattern that produces good average throughput but occasional stragglers.

### Within-iteration vs cross-fork variance

The default `forks=1` means all iterations share the same JVM process. Cross-fork variance (JIT warmup differences, GC
profile differences across forks) doesn't get exposed. `forks=2` is the minimum that exposes cross-fork variance at all;
for published numbers use `forks=5` (see [Recommended publishing run](#recommended-publishing-run) — the 2026-06-20
fork=2 capture produced 25%+ error bars and NaN score-errors).

## Caveats

A few things to keep in mind whenever you quote a number from this suite:

- **Broker runs in Docker (Testcontainers Kafka 4.3.0), not in-process.** That's the right call for a competitive bench
  — the broker isn't fighting consumers for cores — but it also means network IO is real (loopback to a container) and
  absolute numbers are still **lower than a production setup with a network broker on dedicated hardware**. The headline
  _ordering_ between runtimes is what's meaningful; absolute records-per-second is a function of how much of the host's
  cores Docker / the container runtime is giving the broker.
- **Single-broker, single-container.** No replication, no leader election, no rebalance under load. The broker container
  runs replication factor 1 and `min.insync.replicas=1`. Real failure modes won't show up.
- **Single payload size.** The seed payload is small JSON (~50 bytes). Reactor Kafka's `flatMap` strategy can behave
  differently under 10KB payloads.
- **Macros vs micros.** This suite measures consumer-side throughput. End-to-end latency (produce → consume → process →
  ack) needs a separate harness.
- **Docker overhead is real.** The first iteration of every trial pays the container-startup cost. JMH's warmup phase
  absorbs most of that; if you're spot-checking with `-wi 0`, expect the first measurement iteration to look slow.
- **Share-group setup is validated end-to-end (Docker, Kafka 4.3.0).** The share arm needs broker-side KIP-932 support
  (the `share` rebalance protocol + the `__share_group_state` topic, set via container env) and the share group's start
  offset reset to `earliest` (a group config set via Admin — the default `latest` would skip every seeded record and
  time the bench out). A smoke run confirmed the group forms, assigns all 8 partitions, and consumes the seeded records.
  If a future run regresses and the `share` cell times out, check those two things first: broker share coordinator logs,
  and whether `share.auto.offset.reset` took.
- **The `share` arm looks protocol-bound, not work-bound.** In the publishing runs (see `results/2026-05-29.md` and
  `results/2026-05-30.md`) its throughput is roughly flat across `workMicros` — the share-acquire/ack path (and
  `__share_group_state` writes per ack batch) dominates, not the per-record work. Tune poll batch size / ack batching
  and re-measure before quoting share numbers, and never read the share ↔ KEY_ORDERED gap as an ordering verdict — they
  are not rivals.

## When the numbers should change

Re-publish the results file when **any** of these change:

- A new runtime is added or an existing one is upgraded (Confluent PC version bump, Reactor Kafka version bump, Kafka
  client version bump).
- KPipe's hot-path code changes (offset manager, pipeline builder, consumer thread loop).
- The JMH config changes (different iteration count, different fork count).
- The hardware changes.

A single number without a date is misleading. The `results/` directory holds dated snapshots.
