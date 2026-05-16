# Benchmark snapshot — YYYY-MM-DD

Copy this file when publishing a fresh benchmark run. Fill in every section. If a section
doesn't apply (e.g., no `perfnorm` data because the run was on macOS), say so explicitly.

## Environment

| Field | Value |
|---|---|
| Date (UTC) | `YYYY-MM-DD HH:MM` |
| Hardware | e.g., `Apple Silicon M2 Pro, 10 cores, 16 GB` |
| OS / Kernel | e.g., `macOS 14.5 (Darwin 24.6.0)` |
| JDK | `java -version` output |
| Kafka | `4.2.0` (in-process via Apache Kafka test-kit) |
| KPipe | `git rev-parse HEAD` |
| Confluent Parallel Consumer | from `gradle/libs.versions.toml` |
| Reactor Kafka | from `gradle/libs.versions.toml` |

## JMH configuration

| Parameter | Value |
|---|---|
| Warmup iterations | 3 (default) |
| Measurement iterations | 5 (default) |
| Forks | 2 (recommended for publishing) |
| Profilers | `gc` |
| Result format | `JSON` (artifact attached: `results-YYYY-MM-DD.json`) |

## Scope

```bash
./gradlew :benchmarks:jmh \
  -Pjmh.includes='ParallelProcessingBenchmark|ParallelProcessingLatencyBenchmark' \
  -Pjmh.fork=2 \
  -Pjmh.profilers='gc' \
  -Pjmh.resultFormat=JSON
```

## Throughput — ParallelProcessingBenchmark

Records / second, higher is better. `workMicros` is per-record simulated work via
`LockSupport.parkNanos`.

| Runtime | workMicros=0 | workMicros=100 | workMicros=1000 |
|---|---:|---:|---:|
| KPipe | TBD ± TBD | TBD ± TBD | TBD ± TBD |
| Confluent Parallel Consumer | TBD ± TBD | TBD ± TBD | TBD ± TBD |
| Reactor Kafka | TBD ± TBD | TBD ± TBD | TBD ± TBD |
| Raw `KafkaConsumer` + VT | TBD ± TBD | TBD ± TBD | TBD ± TBD |

### Observations

- (Which runtime leads at `workMicros=0`? What's the gap?)
- (Where does the leader change as `workMicros` increases?)
- (Anything surprising in the error band?)

## Latency percentiles — ParallelProcessingLatencyBenchmark

Per-record time (ms), lower is better. Reported at `workMicros=100` (typical local enrichment).

| Runtime | p50 | p95 | p99 |
|---|---:|---:|---:|
| KPipe | TBD | TBD | TBD |
| Confluent Parallel Consumer | TBD | TBD | TBD |
| Reactor Kafka | TBD | TBD | TBD |
| Raw `KafkaConsumer` + VT | TBD | TBD | TBD |

### Observations

- (Does the throughput leader also have the best tail?)
- (Where does the p99 gap widen?)

## Allocation profile (GC profiler)

`gc.alloc.rate.norm` (B / op, lower is better) at `workMicros=100`.

| Runtime | B/op |
|---|---:|
| KPipe | TBD |
| Confluent Parallel Consumer | TBD |
| Reactor Kafka | TBD |
| Raw `KafkaConsumer` + VT | TBD |

## Batch sink

See `BatchSinkLatencyBenchmark` — single-runtime, separate sweep. Headline:

| batchSize | sinkLatencyMicros | Throughput (ops/s) |
|---:|---:|---:|
| 1 | 10 | TBD |
| 1 | 1000 | TBD |
| 100 | 10 | TBD |
| 100 | 1000 | TBD |

## Raw output

The full JMH JSON output is attached as `results-YYYY-MM-DD.json`. The `gc` profiler section in
that file is the source of truth for allocation rates.

## Pitfalls in this run

(Anything weird? A flaky iteration? A different OS than the reference? Note it here.)
