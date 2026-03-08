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

Evaluates the throughput of KPipe's Java 24 Virtual Thread-based parallel processing engine against the Confluent
Parallel Consumer.

- **KPipe Parallel Mode**: Leverages a thread-per-record model using Loom to process message batches concurrently with
  minimal overhead.
- **Confluent Parallel Consumer**: Industry-standard library for parallel processing, used as a baseline for comparison.
- **Kafka backend**: Uses an in-process embedded Kafka broker powered by Apache Kafka test kit.

## Running Benchmarks

To run all benchmarks with default JMH settings:

```bash
./gradlew :benchmarks:jmh
```

### Running Specific Benchmarks

You can use regex to target specific benchmark classes:

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

### Parallel Processing (`ParallelProcessingBenchmark`)

Run date: `2026-03-08`

| Benchmark                                                 |    Mode |  Cnt |     Score |       Error |   Units |
|-----------------------------------------------------------|--------:|-----:|----------:|------------:|--------:|
| `ParallelProcessingBenchmark.confluentParallelProcessing` | `thrpt` | `16` | `330.210` | `+/- 0.202` | `ops/s` |
| `ParallelProcessingBenchmark.kpipeParallelProcessing`     | `thrpt` | `16` | `331.298` | `+/- 0.529` | `ops/s` |

Quick read: both are effectively at parity for this run configuration.

Reproduce this benchmark family:

```bash
INCLUDES='ParallelProcessingBenchmark' ./run_benchmarks.sh
```

## Understanding Results

The benchmarks typically run in `Throughput` mode (`ops/s`). Higher numbers are better.

Key performance indicators to watch for:

- **SerDe Tax**: The drop in throughput as more transformation steps are added in the manual vs. optimized KPipe
  pipeline.
- **GC Pressure**: While not explicitly measured by throughput, the zero-copy Avro benchmark significantly reduces
  memory allocation and garbage collection overhead.
- **Concurrency Scaling**: How the parallel processing benchmark handles large batches compared to sequential
  processing.
- **Real Infrastructure vs. Mocks**: This suite favors repeatable local microbenchmarks by using an embedded Kafka
  broker.
- **Parallel timing fairness**: both `kpipeParallelProcessing` and `confluentParallelProcessing` start
  their processing loops inside benchmark methods (not in setup), so measured time includes comparable
  startup-to-completion behavior for each invocation.
- **Parallel throughput normalization**: `ParallelProcessingBenchmark` uses `@OperationsPerInvocation(1000)`, so its
  reported throughput is normalized per processed message rather than per full benchmark invocation.
- **Logging noise control**: KPipe parallel benchmark uses a no-op sink in benchmark runs to avoid console I/O from
  distorting throughput numbers.

## Requirements

- **Java 24+**: Required for Virtual Threads (Project Loom).
- **Gradle**: Used to compile and execute the benchmark harness.
