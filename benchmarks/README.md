# KPipe Benchmarks

JMH benchmarks for KPipe — pipeline efficiency, Avro byte handling, parallel processing, and batch sink throughput. Each
scenario pits KPipe against either a hand-rolled equivalent or a well-known alternative (Confluent Parallel Consumer) so
the numbers mean something concrete.

## Benchmark Scenarios

### 1. JSON Pipeline Efficiency (`JsonPipelineBenchmark`)

Compares KPipe's optimized "Single SerDe Cycle" against traditional byte-to-byte transformation chaining.

- **KPipe JSON Pipeline**: Deserializes a `byte[]` once, applies multiple `UnaryOperator<Map<String, Object>>`
  transformations on the same object, and serializes once back to `byte[]`.
- **Manual SerDe Chained**: Mimics a "naive" pipeline where each transformation step independently deserializes the
  input and re-serializes the output (`byte[] -> Object -> byte[]`).

### 2. Avro Zero-Copy Handling (`AvroPipelineBenchmark`)

Measures the efficiency of KPipe's magic byte offset handling vs. traditional byte array copying.

- **KPipe Avro Magic Pipeline**: Uses the `offset` parameter in `processAvro` to skip Confluent's 5-byte magic prefix
  without creating an intermediate array copy.
- **Manual Avro Magic Handling**: Strips the magic bytes using `Arrays.copyOfRange` before deserialization.

### 3. Parallel Processing — Competitive Suite (`ParallelProcessingBenchmark`)

Four parallel-consumer runtimes drink from the same seeded topic on a **Testcontainers-managed Kafka 4.3.0 broker**
(Docker; broker runs in its own JVM on its own cores so it isn't fighting the consumer under test for CPU):

- **KPipe** — virtual-thread-per-record on Loom; no pool, no queue.
- **Confluent Parallel Consumer** — `ProcessingOrder.UNORDERED` with a 100-worker pool. The de-facto industry baseline.
- **Reactor Kafka** — `Flux<ReceiverRecord>` on the Reactor `parallel` scheduler with a matching concurrency limit.
- **Raw `KafkaConsumer` + `newVirtualThreadPerTaskExecutor`** — the hand-rolled baseline. No framework, no offset
  manager. Establishes the floor of "what if I just wrote the loop myself?"

A `workMicros` `@Param` (`0`, `100`, `1000`) injects per-record work via `LockSupport.parkNanos` so the bench covers
three workload regimes: pure framework overhead (0 µs), local enrichment (100 µs), and blocking I/O round trip (1000
µs). At `workMicros=0` the comparison is "who has the lowest per-record overhead?"; at `workMicros=1000` it's "who
schedules blocking work best?" — those two questions usually have different winners.

Each invocation processes **25,000 records** across **8 partitions**. Because the broker is in its own container (not
in-process), pushing the record count higher actually scales the consumer workload instead of bottlenecking on broker
contention. **Docker must be running** before invoking the bench; Testcontainers will pull `apache/kafka:4.3.0` and
start the container at trial setup.

### 4. Batch Sink Throughput (`BatchSinkLatencyBenchmark`)

Quantifies the win from `Stream.toBatch(...)` when the destination has nontrivial per-call cost. The benchmark
parameterises over batch size (1 / 10 / 100) and a simulated sink latency (10 / 100 / 1000 µs) — the sink calls
`LockSupport.parkNanos(latency)` once per batch regardless of size, mirroring JDBC commits, HTTP POSTs, and S3 PUTs
where wall-clock cost is dominated by a per-call round trip rather than per-record CPU.

A no-op sink would give a misleading result here (per-record overhead becomes the only cost), so the bench deliberately
inserts the per-call latency to expose the amortisation that batching provides.

#### Results (Apple Silicon, JDK 25.0.2, single fork, 3×2s warmup + 5×3s measurement)

| batchSize | sinkLatencyMicros | Throughput (ops/s) | Speedup vs batchSize=1 |
| --------: | ----------------: | -----------------: | ---------------------: |
|         1 |                10 |             28,018 |                  1.00× |
|         1 |               100 |              6,885 |                  1.00× |
|         1 |              1000 |                788 |                  1.00× |
|        10 |                10 |            264,606 |                   9.4× |
|        10 |               100 |             63,813 |                   9.3× |
|        10 |              1000 |              7,778 |                   9.9× |
|       100 |                10 |            605,458 |                  21.6× |
|       100 |               100 |            361,636 |                  52.5× |
|       100 |              1000 |             66,059 |                  83.9× |

The story is exactly what the design promised: at 1000 µs/call (think a slow JDBC commit) batching at size 100 is **~84×
the throughput** of single-record processing. At 10 µs/call batching still helps, but the gain shrinks because per-call
cost is no longer the bottleneck.

## Running Benchmarks

To run all benchmarks with default JMH settings:

```bash
./gradlew :benchmarks:jmh
```

### Running Specific Benchmarks

You can use regex to target specific benchmark classes using the `jmh.includes` property:

```bash
# Run only JSON benchmarks
./gradlew :benchmarks:jmh -Pjmh.includes='JsonPipelineBenchmark'

# Run only Avro benchmarks
./gradlew :benchmarks:jmh -Pjmh.includes='AvroPipelineBenchmark'

# Run only Parallel Processing benchmarks
./gradlew :benchmarks:jmh -Pjmh.includes='ParallelProcessingBenchmark'
```

### Adjusting Benchmark Parameters

JMH parameters can be configured in `benchmarks/build.gradle.kts` or passed via the command line:

```bash
# Example: 1 iteration, 1 warmup, 1 fork
./gradlew :benchmarks:jmh -Pjmh.iterations=1 -Pjmh.warmupIterations=1 -Pjmh.fork=1
```

## Latest Results (Snapshot)

Run date: `2026-05-09` · Apple Silicon · JDK 25.0.2 · single fork · 3×2s warmup + 5×3s measurement (`Cnt = 5`).

### 1. Avro Pipeline: The "Zero-Copy" Advantage

This benchmark compares KPipe's zero-copy offset-based deserialization against the traditional approach of using
`Arrays.copyOfRange`.

> **Bench fix in this snapshot.** The previous snapshot's headline numbers (~740M ops/s / ~351M ops/s) were not real —
> `@Setup` wrote to a `BinaryEncoder` without calling `flush()`, so `out.toByteArray()` returned an empty array. Earlier
> versions of `AvroFormat.deserialize` silently returned `null` on empty input; the manual path's `serialize(null)` then
> NPE'd, but the older `AvroFormat.serialize(null)` happened to no-op. Both crashes were timed as "fast work," producing
> numbers that looked impressive but measured nothing. Adding `encoder.flush()` in `@Setup` produces the realistic
> numbers below.

| Benchmark                                       |    Mode | Cnt |        Score |          Error |   Units |
| ----------------------------------------------- | ------: | --: | -----------: | -------------: | ------: |
| `AvroPipelineBenchmark.kpipeAvroMagicPipeline`  | `thrpt` | `5` | `926,454.03` | `± 255,156.10` | `ops/s` |
| `AvroPipelineBenchmark.kpipeAvroPipeline`       | `thrpt` | `5` | `806,103.53` | `± 308,309.30` | `ops/s` |
| `AvroPipelineBenchmark.manualAvroMagicHandling` | `thrpt` | `5` | `676,133.95` | `± 662,309.38` | `ops/s` |

**Observation**: KPipe's zero-copy magic-byte handling is **~1.37× faster** than the manual `Arrays.copyOfRange`
strip-and-reparse approach. The error band on the manual path is wide because allocation pressure interacts with GC
pauses unpredictably; the gap is real but smaller than the previous (broken) snapshot's `~2.1×` claim.

### 2. JSON Pipeline: Defeating the "SerDe Tax"

This benchmark measures the cost of chaining multiple transformations.

| Benchmark                                      |    Mode | Cnt |        Score |          Error |   Units |
| ---------------------------------------------- | ------: | --: | -----------: | -------------: | ------: |
| `JsonPipelineBenchmark.kpipeJsonPipeline`      | `thrpt` | `5` | `241,973.45` | `± 112,445.20` | `ops/s` |
| `JsonPipelineBenchmark.manualJsonSerDeChained` | `thrpt` | `5` |  `78,396.78` |  `± 38,829.91` | `ops/s` |
| `JsonPipelineBenchmark.manualJsonSingleSerDe`  | `thrpt` | `5` | `200,700.24` | `± 202,444.52` | `ops/s` |

**Observation**: KPipe is **~3.1× faster** than a naive chained approach (`manualJsonSerDeChained`) and roughly on par
with the manual single-SerDe implementation (`manualJsonSingleSerDe`) — the latter overlaps within error in this
single-fork run. Both confirm KPipe's operator chaining adds no measurable SerDe tax on top of a bespoke single-pass
implementation.

### 3. Parallel Processing: Virtual Threads (Loom) vs. Confluent

This benchmark compares KPipe's "thread-per-record" model using Java Virtual Threads against the industry-standard
Confluent Parallel Consumer.

| Benchmark                                                 |    Mode | Cnt |       Score |       Error |   Units |
| --------------------------------------------------------- | ------: | --: | ----------: | ----------: | ------: |
| `ParallelProcessingBenchmark.confluentParallelProcessing` | `thrpt` | `5` | `3,110.974` | `± 400.967` | `ops/s` |
| `ParallelProcessingBenchmark.kpipeParallelProcessing`     | `thrpt` | `5` | `3,268.037` |  `± 83.313` | `ops/s` |

**Observation**: With `10,000` messages per invocation and `8` partitions, this run shows a measurable throughput edge
for KPipe (**~5.0%** over Confluent) with notably tighter variance (`±83` vs `±401`). KPipe's score is also more stable
across iterations — Confluent's wider error band reflects iteration-to-iteration spread on the embedded Kafka broker.

### 4. Batch Sink Throughput

See § 4 above (`BatchSinkLatencyBenchmark`) for the size × latency parameter sweep. Headline: at
`sinkLatencyMicros=1000` (≈ JDBC commit), `batchSize=100` yields **~84× the throughput** of `batchSize=1`.

## Reading the numbers

Throughput mode (`ops/s`). Bigger is better.

### Projected messages per second

- **Avro (In-Memory)**: Up to **~926,000 records/s** with zero-copy magic-byte handling. JSON beats this in absolute
  ops/s here because the JSON bench applies two operators per record while the Avro bench applies two operators on a
  larger payload — comparing benches with different shapes is unfair, but the within-benchmark KPipe-vs-manual ratio is
  the headline number.
- **JSON (In-Memory)**: Up to **~242,000 records/s** with the KPipe single-SerDe pipeline (3.1× the chained-SerDe
  baseline).
- **End-to-End Parallel Processing**: **~3,111 to ~3,268 records/s**. `ParallelProcessingBenchmark` uses
  `@OperationsPerInvocation(TARGET_MESSAGES)`, so JMH already reports processed records per second.
- **Batch Sink (Slow Destination)**: At a 1ms-per-call simulated sink, batching at size 100 reaches **~66,000 batches/s
  ≈ 6.6M records/s**, vs **~788 records/s** for size 1. Batching wins big when destination cost dominates.

> **Note**: The `ParallelProcessingBenchmark` uses `@OperationsPerInvocation(TARGET_MESSAGES)` where
> `TARGET_MESSAGES = 25_000`. JMH already normalises for that, so the reported `ops/s` is the message rate.

A few things worth keeping in mind when comparing runs:

- **SerDe tax** shows up as throughput drop-off when you chain more transformations on the manual pipeline vs. KPipe's
  single-deserialize approach.
- **GC pressure** isn't on the throughput chart but matters for the Avro zero-copy bench — fewer copies means fewer
  allocations means less GC noise.
- **Concurrency scaling** between parallel and sequential modes only tells you something useful when the workload fits
  more than one core's worth of work.
- **Embedded Kafka** keeps runs repeatable and fast, at the cost of not exercising real network/SSL paths.
- **Timing fairness for parallel benches**: both `kpipeParallelProcessing` and `confluentParallelProcessing` run their
  processing loops inside the benchmark method (not `@Setup`), so startup-to-completion is measured the same way for
  both.
- **Throughput normalization**: `ParallelProcessingBenchmark` uses `@OperationsPerInvocation(TARGET_MESSAGES)`, so the
  reported `ops/s` is already per processed message, not per full invocation.
- **Logging noise**: the KPipe parallel bench uses a no-op sink so console I/O doesn't bend the numbers.
- **CPU counters (Linux only)**: `perfnorm` gives normalized CPI / cycles / instructions. macOS can't supply those via
  JMH, so don't quote CPI on a macOS run.

## Requirements

- **Java 24+**
- **Gradle**: Used to compile and execute the benchmark harness.

### CPU/CPI Profiling For Parallel Benchmark

For KPipe vs Confluent parallel processing, keep the benchmark target fixed and enable a profiler:

```bash
# Linux: collect normalized hardware counters (includes CPI)
./gradlew :benchmarks:jmh \
  -Pjmh.includes='ParallelProcessingBenchmark' \
  -Pjmh.profilers='perfnorm' \
  -Pjmh.resultFormat=TEXT

# macOS: CPI is not available via perf counters in JMH; use GC/CPU-adjacent signal instead
./gradlew :benchmarks:jmh \
  -Pjmh.includes='ParallelProcessingBenchmark' \
  -Pjmh.profilers='gc' \
  -Pjmh.resultFormat=TEXT
```

You can also use the helper script:

```bash
# Linux (CPI mode)
PROFILE_MODE=cpi INCLUDES='ParallelProcessingBenchmark' ./scripts/run-benchmarks.sh

# macOS (falls back from cpi -> gc with a warning)
PROFILE_MODE=cpi INCLUDES='ParallelProcessingBenchmark' ./scripts/run-benchmarks.sh

# Heap/allocation view (portable)
PROFILE_MODE=heap INCLUDES='ParallelProcessingBenchmark' ./scripts/run-benchmarks.sh

# Thread/runtime view (HotSpot)
PROFILE_MODE=threads INCLUDES='ParallelProcessingBenchmark' ./scripts/run-benchmarks.sh
```

Supported `PROFILE_MODE` values in `scripts/run-benchmarks.sh`:

- `none`: no JMH profiler
- `gc`: allocation and GC counters (`gc`)
- `heap`: allocation/GC plus HotSpot GC internals (`gc,hs_gc`)
- `threads`: HotSpot thread/runtime signal (`hs_thr,hs_rt`)
- `cpi`: Linux `perfnorm` (falls back to `gc` on macOS)

Interpretation guidance for KPipe vs Confluent:

- Throughput (`ops/s`) remains the primary metric.
- On Linux, `perfnorm` adds normalized counters; compare CPI (`cycles`/`instructions`) between both benchmarks.
- Lower CPI at similar throughput usually indicates better instruction-path efficiency.
- On macOS, use throughput plus GC metrics; do not claim CPI without Linux perf counters.

### Parallel Comparison Graph

The latest visual comparison for KPipe vs Confluent parallel processing is at:

- `benchmarks/graphs/parallel_processing_gc_comparison.svg`

![KPipe vs Confluent Parallel Benchmark](graphs/parallel_processing_gc_comparison.svg)

To regenerate source benchmark results before producing/refreshing the graph:

```bash
./gradlew :benchmarks:jmh \
  -Pjmh.includes='ParallelProcessingBenchmark' \
  -Pjmh.profilers='gc' \
  -Pjmh.resultFormat=TEXT
```

> Note: JMH output is written to `benchmarks/build/results/jmh/results.<resultFormat lowercase>`. For `TEXT`, the file
> is typically `benchmarks/build/results/jmh/results.text`.
