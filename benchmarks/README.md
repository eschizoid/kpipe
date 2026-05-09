# KPipe Benchmarks

This module contains a suite of JMH (Java Microbenchmark Harness) tests to quantify KPipe's performance across different
scenarios and compare it against manual implementations and industry-standard alternatives.

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

### 3. Parallel Processing Overhead (`ParallelProcessingBenchmark`)

Evaluates the throughput of KPipe's Java Virtual Thread-based parallel processing engine against the Confluent Parallel
Consumer.

- **KPipe Parallel Mode**: Leverages a thread-per-record model using Loom to process message batches concurrently with
  minimal overhead.
- **Confluent Parallel Consumer**: Industry-standard library for parallel processing, used as a baseline for comparison.
- **Kafka backend**: Uses an in-process embedded Kafka broker powered by Apache Kafka test kit.

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

| Benchmark                                       |    Mode | Cnt |        Score |           Error |   Units |
|-------------------------------------------------|--------:|----:|-------------:|----------------:|--------:|
| `AvroPipelineBenchmark.kpipeAvroMagicPipeline`  | `thrpt` | `5` | `926,454.03` | `± 255,156.10`  | `ops/s` |
| `AvroPipelineBenchmark.kpipeAvroPipeline`       | `thrpt` | `5` | `806,103.53` | `± 308,309.30`  | `ops/s` |
| `AvroPipelineBenchmark.manualAvroMagicHandling` | `thrpt` | `5` | `676,133.95` | `± 662,309.38`  | `ops/s` |

**Observation**: KPipe's zero-copy magic-byte handling is **~1.37× faster** than the manual `Arrays.copyOfRange`
strip-and-reparse approach. The error band on the manual path is wide because allocation pressure interacts with GC
pauses unpredictably; the gap is real but smaller than the previous (broken) snapshot's `~2.1×` claim.

### 2. JSON Pipeline: Defeating the "SerDe Tax"

This benchmark measures the cost of chaining multiple transformations.

| Benchmark                                      |    Mode | Cnt |        Score |          Error |   Units |
|------------------------------------------------|--------:|----:|-------------:|---------------:|--------:|
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
|-----------------------------------------------------------|--------:|----:|------------:|------------:|--------:|
| `ParallelProcessingBenchmark.confluentParallelProcessing` | `thrpt` | `5` | `3,110.974` | `± 400.967` | `ops/s` |
| `ParallelProcessingBenchmark.kpipeParallelProcessing`     | `thrpt` | `5` | `3,268.037` |  `± 83.313` | `ops/s` |

**Observation**: With `10,000` messages per invocation and `8` partitions, this run shows a measurable throughput edge
for KPipe (**~5.0%** over Confluent) with notably tighter variance (`±83` vs `±401`). KPipe's score is also more stable
across iterations — Confluent's wider error band reflects iteration-to-iteration spread on the embedded Kafka broker.

### 4. Batch Sink Throughput

See § 4 above (`BatchSinkLatencyBenchmark`) for the size × latency parameter sweep. Headline: at
`sinkLatencyMicros=1000` (≈ JDBC commit), `batchSize=100` yields **~84× the throughput** of `batchSize=1`.

## Understanding Results

The benchmarks typically run in `Throughput` mode (`ops/s`). Higher numbers are better.

### Projecting "Messages Per Second"

Based on the latest snapshot results, we can derive the following throughput expectations:

- **Avro (In-Memory)**: Up to **~926,000 records/s** with zero-copy magic-byte handling. JSON beats this in absolute
  ops/s here because the JSON bench applies two operators per record while the Avro bench applies two operators on a
  larger payload — comparing benches with different shapes is unfair, but the within-benchmark KPipe-vs-manual ratio is
  the headline number.
- **JSON (In-Memory)**: Up to **~242,000 records/s** with the KPipe single-SerDe pipeline (3.1× the chained-SerDe
  baseline).
- **End-to-End Parallel Processing**: **~31.1M to ~32.7M messages/s**. For this run, use `score * 10,000` because
  `ParallelProcessingBenchmark` uses `@OperationsPerInvocation(10000)`.
- **Batch Sink (Slow Destination)**: At a 1ms-per-call simulated sink, batching at size 100 reaches **~66,000 batches/s
  ≈ 6.6M records/s**, vs **~788 records/s** for size 1. Batching wins big when destination cost dominates.

> **Note**: The `ParallelProcessingBenchmark` uses `@OperationsPerInvocation(10000)`. For this benchmark, derive message
> rate as `ops/s * 10,000`.

Key performance indicators to watch for:

- **SerDe Tax**: The drop-in throughput as more transformation steps are added in the manual vs. optimized KPipe
  pipeline.
- **GC Pressure**: While not explicitly measured by throughput, the zero-copy Avro benchmark significantly reduces
  memory allocation and garbage collection overhead.
- **Concurrency Scaling**: How the parallel processing benchmark handles large batches compared to sequential
  processing.
- **Real Infrastructure vs. Mocks**: This suite favors repeatable local microbenchmarks by using an embedded Kafka
  broker.
- **Parallel timing fairness**: both `kpipeParallelProcessing` and `confluentParallelProcessing` start their processing
  loops inside benchmark methods (not in setup), so measured time includes comparable startup-to-completion behavior for
  each invocation.
- **Parallel throughput normalization**: `ParallelProcessingBenchmark` uses `@OperationsPerInvocation(10000)`, so its
  reported throughput is normalized per processed message rather than per full benchmark invocation.
- **Logging noise control**: KPipe parallel benchmark uses a no-op sink in benchmark runs to avoid console I/O from
  distorting throughput numbers.
- **CPU efficiency (Linux only)**: compare CPI and related normalized counters from `perfnorm` for
  `kpipeParallelProcessing` vs `confluentParallelProcessing`.
- **Platform caveat for CPI**: macOS runs can still compare throughput and GC behavior, but CPI should be
  collected/reported only from Linux perf-enabled runs.

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
