# Benchmark snapshot — 2026-10-23 CI latency capture

`ParallelProcessingLatencyBenchmark` for the comparison arms, with five forks, JMH 1.37, and KPipe committing offsets.
This replaces the 2026-10-15 latency runs, which used two forks and ran KPipe without offset tracking and Reactor on a
one-thread-per-CPU scheduler.

Each operation drains one batch of 25,000 records. JMH reports milliseconds per record because every arm uses
`@OperationsPerInvocation`; the tables multiply them back by 25,000, so every figure is milliseconds per batch, lower is
better. The figures describe whole batches, not the latency of single records, which `PerRecordLatencyHarness` measures.

## Environment

| Field         | Value                                                                                                                                                                        |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Date (UTC)    | 2026-10-23 11:24 → 15:08 (unordered), 11:24 → 13:37 (per-key)                                                                                                                |
| Hardware      | GitHub-hosted runner, standard public-repository size (4 vCPU)                                                                                                               |
| OS            | Ubuntu 24.04                                                                                                                                                                 |
| JDK           | 25.0.4                                                                                                                                                                       |
| JMH           | 1.37                                                                                                                                                                         |
| Kafka broker  | 4.3.0 in a Testcontainers container (`apache/kafka:4.3.0`)                                                                                                                   |
| kafka-clients | 4.3.0, forced for every arm in `benchmarks/build.gradle.kts`                                                                                                                 |
| KPipe         | `main` at `b92c3b38`                                                                                                                                                         |
| Atleon        | 0.47.1, `AcknowledgementQueueMode.COMPACT_EAGER`                                                                                                                             |
| Workflow runs | [39945896904](https://github.com/eschizoid/kpipe/actions/runs/39945896904) (unordered), [39945900682](https://github.com/eschizoid/kpipe/actions/runs/39945900682) (per-key) |

JMH ran with 5 forks, 3 warmup iterations and 5 measurement iterations, in `AverageTime` and `SampleTime` modes. The
unordered and per-key arms ran in two jobs on separate runners at the same time, because together they do not fit the
360-minute job limit, so compare arms within a table, not across the two.

## Unordered arms

Average, with the JMH 99.9% error:

| Runtime                                 | workMicros=0 | workMicros=100 | workMicros=1000 |
| --------------------------------------- | -----------: | -------------: | --------------: |
| Raw `KafkaConsumer` + virtual threads   |   26.3 ± 2.3 |     35.4 ± 2.8 |      37.0 ± 2.6 |
| KPipe `PARALLEL`                        |   35.3 ± 2.7 |     48.7 ± 3.9 |      46.3 ± 3.0 |
| Atleon parallel                         |   37.2 ± 1.0 |     45.5 ± 2.0 |      46.3 ± 1.6 |
| Confluent Parallel Consumer `UNORDERED` |  216.2 ± 4.5 |    258.5 ± 3.7 |     364.0 ± 4.3 |
| Reactor Kafka                           |   31.3 ± 1.6 |     71.7 ± 1.7 |     294.8 ± 2.3 |

Percentiles from `SampleTime`, pooled across the five forks:

| Runtime                                 | workMicros |   p50 |   p99 | p99.9 |
| --------------------------------------- | ---------: | ----: | ----: | ----: |
| Raw `KafkaConsumer` + virtual threads   |          0 |  24.8 |  46.6 |  56.5 |
| Raw `KafkaConsumer` + virtual threads   |        100 |  35.3 |  59.0 |  71.0 |
| Raw `KafkaConsumer` + virtual threads   |       1000 |  35.9 |  60.3 |  78.6 |
| KPipe `PARALLEL`                        |          0 |  34.8 |  63.8 | 115.4 |
| KPipe `PARALLEL`                        |        100 |  45.0 |  69.9 | 116.6 |
| KPipe `PARALLEL`                        |       1000 |  45.0 |  77.8 |  96.6 |
| Atleon parallel                         |          0 |  34.4 |  76.3 | 133.5 |
| Atleon parallel                         |        100 |  42.2 |  87.4 | 142.6 |
| Atleon parallel                         |       1000 |  43.1 |  85.8 | 150.8 |
| Confluent Parallel Consumer `UNORDERED` |          0 | 212.0 | 258.0 | 287.2 |
| Confluent Parallel Consumer `UNORDERED` |        100 | 254.0 | 300.2 | 322.4 |
| Confluent Parallel Consumer `UNORDERED` |       1000 | 358.0 | 404.5 | 424.8 |
| Reactor Kafka                           |          0 |  28.6 |  57.1 |  68.7 |
| Reactor Kafka                           |        100 |  70.5 |  97.8 | 114.3 |
| Reactor Kafka                           |       1000 | 296.4 | 313.4 | 331.2 |

KPipe `PARALLEL` and Atleon parallel are within each other's error bands on average at every delay, and KPipe's p99 is
lower at every delay (63.8 against 76.3 ms at 0µs, 69.9 against 87.4 at 100µs, 77.8 against 85.8 at 1000µs). Both stay
within about 10 ms of the raw loop on average once records carry work. Reactor is the fastest framework arm at 0µs and
falls behind as work grows, because its 100 rails process at most 100 records at a time. Confluent Parallel Consumer
takes 5 to 8 times as long as KPipe.

## Per-key ordered arms

Average, with the JMH 99.9% error:

| Runtime                           | workMicros=0 | workMicros=100 | workMicros=1000 |
| --------------------------------- | -----------: | -------------: | --------------: |
| KPipe `KEY_ORDERED`               |   34.2 ± 2.7 |     41.7 ± 2.3 |      74.2 ± 3.2 |
| Atleon key-ordered                |   25.9 ± 1.0 |     40.1 ± 1.0 |      66.6 ± 1.3 |
| Confluent Parallel Consumer `KEY` |  219.5 ± 3.7 |    231.0 ± 3.0 |     363.1 ± 2.4 |

Percentiles from `SampleTime`, pooled across the five forks:

| Runtime                           | workMicros |   p50 |   p99 | p99.9 |
| --------------------------------- | ---------: | ----: | ----: | ----: |
| KPipe `KEY_ORDERED`               |          0 |  31.9 |  49.3 |  60.9 |
| KPipe `KEY_ORDERED`               |        100 |  43.3 |  66.8 |  70.8 |
| KPipe `KEY_ORDERED`               |       1000 |  72.1 |  95.8 | 104.2 |
| Atleon key-ordered                |          0 |  23.1 |  60.5 | 120.2 |
| Atleon key-ordered                |        100 |  38.5 |  71.9 | 134.6 |
| Atleon key-ordered                |       1000 |  64.1 |  91.4 | 157.2 |
| Confluent Parallel Consumer `KEY` |          0 | 215.6 | 246.0 | 258.0 |
| Confluent Parallel Consumer `KEY` |        100 | 228.8 | 260.4 | 271.6 |
| Confluent Parallel Consumer `KEY` |       1000 | 372.0 | 404.7 | 417.6 |

Atleon key-ordered is faster on average at 0µs (25.9 against 34.2 ms, error bands apart) and at 1000µs (66.6 against
74.2, apart); the two are level at 100µs. KPipe's p99 is lower at 0 and 100µs (49.3 against 60.5, 66.8 against 71.9) and
higher at 1000µs (95.8 against 91.4), and its p99.9 is lower at every delay. KPipe takes 3 to 6 times less time than
Confluent Parallel Consumer's per-key mode. The 0µs gap matches the throughput capture of 2026-10-22, where Atleon also
leads KPipe `KEY_ORDERED` at 0µs; its cause is not yet measured and is tracked in #376.

## Raw output

[`2026-10-23-ci-latency.json`](2026-10-23-ci-latency.json) and
[`2026-10-23-ci-latency-key.json`](2026-10-23-ci-latency-key.json) are the full JMH output.
