# KPipe Benchmarks

JMH benchmarks (plus one non-JMH latency harness) for KPipe. Two design-validation benches pit KPipe against a
hand-rolled straw man; a competitive suite pits KPipe against the alternatives a JVM team would actually pick (Kafka
Share Consumer, Confluent Parallel Consumer, Reactor Kafka, a raw `KafkaConsumer + virtual threads`, and a
single-threaded consumer); and three micro-benches isolate internals.

**The numbers live in [`results/`](results/), dated per capture, not in this file.** Embedding a single run here is how
the old README drifted out of sync. Read [`METHODOLOGY.md`](METHODOLOGY.md) before quoting any figure — it explains what
each bench isolates, why fork count matters, and which comparisons are honest.

## Benchmark scenarios

### Design validation (in-memory, no broker)

These are KPipe-vs-straw-man. They show the design choices paid off; they are **not** competitive claims.

| Bench                   | `@Benchmark` arms                                                      | What it isolates                                                                                          |
| ----------------------- | --------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `JsonPipelineBenchmark` | `kpipeJsonPipeline`, `manualJsonSingleSerDe`, `manualJsonSerDeChained` | KPipe's single-SerDe-cycle vs a naive byte→object→byte chain that re-serializes between every operator.   |
| `AvroPipelineBenchmark` | `kpipeAvroMagicPipeline`, `kpipeAvroPipeline`, `manualAvroMagicHandling` | Zero-copy magic-byte offset handling vs stripping the 5-byte Confluent prefix with `Arrays.copyOfRange`. |

### Competitive suite (Testcontainers `apache/kafka:4.3.0`, Docker required)

The broker runs in its own container (its own JVM, its own cores) so it isn't fighting the consumer under test for CPU.
Each invocation processes **`TARGET_MESSAGES = 25,000`** records across **`TOPIC_PARTITIONS = 8`** partitions, and every
arm carries `@OperationsPerInvocation(TARGET_MESSAGES)` so JMH reports processed **records/s** directly. A `workMicros`
`@Param` — `{0, 100, 1000, 10000, 35000, 50000, 100000}` — injects per-record work via `LockSupport.parkNanos`, sweeping
from pure framework overhead (0 µs) through local enrichment to multi-ms I/O hops. At `workMicros=0` the question is
"who has the lowest per-record overhead?"; in the 10–100 ms regime it's "who schedules blocking work best?" — usually
different winners.

**`ParallelProcessingBenchmark` — throughput, 9 arms:**

| Arm                  | Runtime                                                         | Ordering      |
| -------------------- | -------------------------------------------------------------- | ------------- |
| `kpipe`              | KPipe `PARALLEL` — virtual-thread-per-record (Loom), no pool   | none          |
| `kpipeKeyOrdered`    | KPipe `KEY_ORDERED` — per-key serial queues, VT-drained        | per-key       |
| `share`             | Kafka Share Consumer (KIP-932) + `newVirtualThreadPerTaskExecutor` | none      |
| `confluent`          | Confluent Parallel Consumer, `ProcessingOrder.UNORDERED` (100-worker pool) | none |
| `confluentKey`       | Confluent PC, `ProcessingOrder.KEY`                            | per-key       |
| `confluentPartition` | Confluent PC, `ProcessingOrder.PARTITION`                     | per-partition |
| `reactor`            | Reactor Kafka, `Flux.parallel(100)`                           | none          |
| `raw`                | Raw `KafkaConsumer` + `newVirtualThreadPerTaskExecutor`      | none          |
| `singleThread`       | Single-threaded `KafkaConsumer`, inline processing (the floor) | per-partition |

**`ParallelProcessingLatencyBenchmark` — latency, 4 arms** (`kpipe`, `confluent`, `reactor`, `raw`): declares
`@BenchmarkMode({SampleTime, AverageTime})` and reports p50/p95/p99 of **whole-invocation** completion time (batch
amortized — divide by `TARGET_MESSAGES` for a rough per-record figure).

**`PerRecordLatencyHarness` — true per-record tail latency (NOT JMH).** A plain `main()` harness, because JMH can't
ingest tens of thousands of externally-measured timestamp pairs. Producer stamps each record's intended send slot
(coordinated-omission corrected); the sink measures `now − stamp` after the simulated work, retains every sample in a
`long[]`, and sorts once for exact p50/p95/p99/max. Arms: `kpipeParallel`, `kpipeKeyOrdered`, `confluentUnordered`,
`rawVirtualThreads`. See [Running](#running-benchmarks) below.

### Micro-benchmarks (MockConsumer, no broker)

| Bench                        | Arm / `@Benchmark` | `@Param` sweep                                                | What it isolates                                                                                            |
| ---------------------------- | ------------------ | ------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `BatchSinkLatencyBenchmark`  | `run`              | `sinkLatencyMicros ∈ {10,100,1000}`, `batchSize ∈ {1,10,100}` | The `Stream.toBatch(...)` amortisation when the destination has a per-call cost (`batchSize=1` is the control). |
| `KeyOrderedDispatchBenchmark`| `dispatch`         | `mode ∈ {PARALLEL, KEY_ORDERED}`, key cardinality `{1,100,10000}` | Dispatcher overhead of `KEY_ORDERED` vs `PARALLEL`. Arbitrated the v2 rewrite (CHM + per-queue monitors, +122% at 10k keys — `results/2026-07-21-keyordered-dispatch-ab.md`); stays the gate for future dispatcher work. |
| `OffsetManagerBoxingBenchmark`| `trackAndMark`    | partitions `{8}`                                              | Allocation/boxing cost on the offset-tracking hot path.                                                     |

## Running benchmarks

Run everything with the default JMH settings (warmup 3 · iterations 5 · fork 1 · threads 1):

```bash
./gradlew :benchmarks:jmh
```

Target specific benchmarks with a regex over the fully-qualified name (`@Param` values are **not** part of the FQN and
can't be filtered this way):

```bash
# A design-validation bench (no Docker needed)
./gradlew :benchmarks:jmh -Pjmh.includes='JsonPipelineBenchmark'

# The whole competitive throughput suite (Docker required)
./gradlew :benchmarks:jmh -Pjmh.includes='ParallelProcessingBenchmark'

# A single arm — the `$` anchor keeps `confluent` from also matching `confluentKey`/`confluentPartition`
./gradlew :benchmarks:jmh -Pjmh.includes='ParallelProcessingBenchmark\.kpipe$' -Pjmh.fork=1 -Pjmh.iterations=1
```

Common overrides: `-Pjmh.iterations`, `-Pjmh.warmupIterations`, `-Pjmh.fork`, `-Pjmh.profilers='gc'`,
`-Pjmh.resultFormat=JSON`, and `-Pjmh.workMicros='0,100,1000'` to run only a subset of the `workMicros` cells (CI uses
this to skip the multi-ms regime that blows past the job cap). The full matrix of recommended publishing / CI / smoke
runs is in [`METHODOLOGY.md`](METHODOLOGY.md#jmh-configuration).

Per-record tail latency uses its own task (not `jmh`), and reuses the suite's Testcontainers broker (Docker required):

```bash
./gradlew :benchmarks:perRecordLatency \
  -Platency.workMicros=100 \
  -Platency.ratePerSecond=2000 \
  -Platency.warmupRecords=5000 \
  -Platency.measuredRecords=20000 \
  -Platency.arms=kpipeParallel,kpipeKeyOrdered,confluentUnordered,rawVirtualThreads
```

All properties are optional (values above are the defaults). Results print as a table and land in
`benchmarks/build/results/per-record-latency/results.json`.

## Results

Captured runs live in [`results/`](results/), one dated markdown snapshot (plus raw JSON) per capture — read the most
recent one, and read the **ordering**, not the absolutes, for any CI-runner capture. Every snapshot records the box, JDK,
fork count, and `workMicros` cells per the [publishing checklist](METHODOLOGY.md#what-to-write-down-with-every-published-number).

The latest KPipe-vs-alternatives visual is regenerated from a `gc`-profiled `ParallelProcessingBenchmark` run:

![KPipe vs Confluent Parallel Benchmark](graphs/parallel_processing_gc_comparison.svg)

## Profiling (parallel suite)

Keep the target fixed and attach a profiler. CPI/hardware counters are Linux-only (`perfnorm`); macOS can't supply them
through JMH, so don't quote CPI from a macOS run — use `gc` there instead.

```bash
# Linux: normalized hardware counters (includes CPI = cycles/instructions)
./gradlew :benchmarks:jmh -Pjmh.includes='ParallelProcessingBenchmark' -Pjmh.profilers='perfnorm' -Pjmh.resultFormat=TEXT

# macOS: allocation / GC signal instead
./gradlew :benchmarks:jmh -Pjmh.includes='ParallelProcessingBenchmark' -Pjmh.profilers='gc' -Pjmh.resultFormat=TEXT
```

The `scripts/run-benchmarks.sh` helper wraps these via `PROFILE_MODE` (`none` / `gc` / `heap` / `threads` / `cpi`, the
last falling back to `gc` on macOS):

```bash
PROFILE_MODE=cpi   INCLUDES='ParallelProcessingBenchmark' ./scripts/run-benchmarks.sh
PROFILE_MODE=heap  INCLUDES='ParallelProcessingBenchmark' ./scripts/run-benchmarks.sh
```

> JMH output is written to `benchmarks/build/results/jmh/results.<resultFormat lowercase>` (e.g. `results.json`).

## Requirements

- **Java 25** (the project toolchain; benchmarks compile at `--release 25`).
- **Gradle** — compiles and runs the harness (use the wrapper, `./gradlew`).
- **Docker** — required for the competitive suite (`ParallelProcessingBenchmark`, `ParallelProcessingLatencyBenchmark`)
  and `perRecordLatency`; Testcontainers pulls `apache/kafka:4.3.0`. The design-validation and micro-benches run
  in-memory and need no broker.
