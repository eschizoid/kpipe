# KPipe Benchmark Methodology

This document is how to read the numbers in `benchmarks/results/` and how to reproduce them. If you're tempted to quote
a single number from a single run, read this first.

## What the suite measures

| Bench                                | Question it answers                                                                                                                            |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `JsonPipelineBenchmark`              | What does KPipe's single-SerDe-cycle save vs naive byte-to-byte chaining? (Design validation, not competitive.)                                |
| `AvroPipelineBenchmark`              | What does zero-copy magic-byte handling save vs `Arrays.copyOfRange`? (Design validation.)                                                     |
| `ParallelProcessingBenchmark`        | How does **throughput** compare across KPipe (PARALLEL + KEY_ORDERED), the Kafka Share Consumer (KIP-932), Confluent Parallel Consumer, Reactor Kafka, and a hand-rolled `KafkaConsumer + virtual threads`? |
| `ParallelProcessingLatencyBenchmark` | How do those four runtimes compare on **per-batch latency** (p50, p95, p99)?                                                                   |
| `BatchSinkLatencyBenchmark`          | What does `Stream.toBatch(...)` save when the destination has nontrivial per-call cost?                                                        |

The first two are KPipe-vs-straw-man. They show the design choices paid off but are not competitive claims. Use them in
design docs, not in pitches.

The next two are the competitive suite. **These are the ones to quote** when comparing KPipe to the alternatives a JVM
team would actually pick.

The last one is KPipe alone, parameterised across batch size and sink latency.

## What the parallel runtimes look like

| Runtime                                   | Concurrency primitive                                                                 | Ordering        | Configured concurrency                                                                |
| ----------------------------------------- | ------------------------------------------------------------------------------------- | --------------- | ------------------------------------------------------------------------------------- |
| **KPipe (PARALLEL)**                      | Virtual thread per record (Loom)                                                      | none            | Unbounded (virtual-thread-per-record); the consumer's in-flight watermark caps memory |
| **KPipe (KEY_ORDERED)**                   | Per-key serial queues, each drained by a virtual thread                               | **per-key**     | Unbounded across keys; serial within a key                                            |
| **Kafka Share Consumer (KIP-932)**        | One `KafkaShareConsumer` + `newVirtualThreadPerTaskExecutor`, per-poll-batch barrier  | none            | Unbounded virtual threads; barrier bounds it to one in-flight batch                   |
| **Confluent Parallel Consumer**           | Platform-thread worker pool, `ProcessingOrder.UNORDERED`                              | none            | `maxConcurrency=100`                                                                  |
| **Reactor Kafka**                         | Reactor `parallel` scheduler via `Flux.parallel(N)`                                   | none            | `parallel(100)` to match Confluent's pool size                                        |
| **Raw `KafkaConsumer` + virtual threads** | `Executors.newVirtualThreadPerTaskExecutor()` driven from a platform-thread poll loop | none            | Unbounded virtual threads                                                             |

Loom-based (KPipe PARALLEL/KEY_ORDERED, share, raw) vs platform-threaded (Confluent, Reactor) is the interesting axis. At
`workMicros=0` the comparison is essentially "framework overhead"; at `workMicros=1000` it's "how does each runtime
schedule blocking work?". Those two questions often have different winners.

### Apples-to-apples — read these comparisons honestly

The share consumer and the key-ordered dispatcher do **not** solve the same problem, so the suite pins every variable
except the consumption mechanism (same broker, same seeded data, same `workMicros`, same `@OperationsPerInvocation`
throughput metric, work fanned to virtual threads everywhere) and you read each pairing for what it actually isolates:

- **KPipe PARALLEL vs Share Consumer** is the fair head-to-head: both unordered, both at-least-once, both one process
  with virtual-thread fan-out. The only difference is the fetch+durability protocol — KPipe's fetch + lowest-pending
  offset commit vs the share-acquire + per-record acknowledgement path. The share arm's per-poll-batch barrier (process
  the batch, then poll again, which implicitly ACCEPTs it) is the honest cost of processing a share batch concurrently
  while staying at-least-once, not a handicap.
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
- `ParallelProcessingBenchmark` parameterises over `workMicros` (`0 / 100 / 1000`).

### Recommended publishing run

```bash
./gradlew :benchmarks:jmh \
  -Pjmh.includes='ParallelProcessingBenchmark|ParallelProcessingLatencyBenchmark' \
  -Pjmh.warmupIterations=3 \
  -Pjmh.iterations=5 \
  -Pjmh.fork=2 \
  -Pjmh.profilers='gc' \
  -Pjmh.resultFormat=JSON
```

Two forks instead of one cuts the cross-fork noise on the latency percentiles. The `gc` profiler adds allocation rate
and GC count per benchmark — required input to the "throughput vs allocation-cost" trade-off.

### Quick smoke run (for local sanity-check)

```bash
./gradlew :benchmarks:jmh \
  -Pjmh.includes='ParallelProcessingBenchmark.kpipe.*workMicros=0' \
  -Pjmh.iterations=1 \
  -Pjmh.warmupIterations=1 \
  -Pjmh.fork=1
```

One iteration, one warmup, one fork, only the KPipe / 0-µs cell. Useful for verifying the harness didn't break after
changes; meaningless as a measurement.

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
one record, but the published number is "median time to process one record at this position in the run". Look at the
histogram, not the mean:

- **p50** — typical record. Lower is better.
- **p95 / p99** — tail. This is what matters when SLA-bound consumers are involved.
- **p100** — the worst observation in the run. Highly noisy; don't quote.

A runtime can win on average throughput while losing on p99 — `Flux.parallel(N)` schedules records in a fan-out / fan-in
pattern that produces good average throughput but occasional stragglers.

### Within-iteration vs cross-fork variance

The default `forks=1` means all iterations share the same JVM process. Cross-fork variance (JIT warmup differences, GC
profile differences across forks) doesn't get exposed. For published numbers run with `forks=2` minimum.

## Caveats

A few things to keep in mind whenever you quote a number from this suite:

- **Broker runs in Docker (Testcontainers Kafka 4.2.0), not in-process.** That's the right call for a competitive bench
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
- **Share-group setup is unvalidated until a real Docker run.** The share arm needs broker-side KIP-932 support (the
  `share` rebalance protocol + the `__share_group_state` topic, set via container env) and the share group's start
  offset reset to `earliest` (a group config set via Admin — the default `latest` would skip every seeded record and
  time the bench out). These were written against the 4.2.0 client API but **not run** (no Docker in the authoring
  environment). On the first real run, if the `share` cell times out, check those two things first: broker share
  coordinator logs, and whether `share.auto.offset.reset` took.

## When the numbers should change

Re-publish the results file when **any** of these change:

- A new runtime is added or an existing one is upgraded (Confluent PC version bump, Reactor Kafka version bump, Kafka
  client version bump).
- KPipe's hot-path code changes (offset manager, pipeline builder, consumer thread loop).
- The JMH config changes (different iteration count, different fork count).
- The hardware changes.

A single number without a date is misleading. The `results/` directory holds dated snapshots.
