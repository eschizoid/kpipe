# Benchmark snapshot — 2026-10-15 CI parallel-processing capture

`ParallelProcessingBenchmark` for the nine arms in the comparison graph, captured after the in-flight backpressure fix
in #357. All arms ran in one job on one machine, so every ratio below compares arms from the same run.

## Environment

| Field                       | Value                                                                      |
| --------------------------- | -------------------------------------------------------------------------- |
| Date (UTC)                  | 2026-10-15 08:12 → 10:15                                                   |
| Hardware                    | GitHub-hosted runner, standard public-repository size (4 vCPU)             |
| OS                          | Ubuntu 24.04 (runner image 20261012.334.1)                                 |
| JDK                         | Oracle GraalVM 25.0.4+7-LTS (jvmci-b01)                                    |
| Kafka                       | 4.3.0 in a Testcontainers container (`apache/kafka:4.3.0`)                 |
| KPipe                       | `c4a660d7`                                                                 |
| Atleon                      | 0.47.1                                                                     |
| Confluent Parallel Consumer | 0.5.3.3                                                                    |
| Reactor Kafka               | 1.3.25                                                                     |
| Workflow run                | [38703956384](https://github.com/eschizoid/kpipe/actions/runs/38703956384) |

## JMH configuration

| Parameter              | Value        |
| ---------------------- | ------------ |
| Warmup iterations      | 3            |
| Measurement iterations | 5            |
| Forks                  | 3            |
| Profilers              | `gc`         |
| workMicros             | 0, 100, 1000 |

Three forks rather than five: nine arms at five forks need about 3.3 hours, and the benchmark job stops at 180 minutes.
Splitting the arms across runs would put them on different machines.

Reproduce with the `benchmarks.yaml` workflow, or locally:

```bash
./gradlew :benchmarks:jmh \
  -Pjmh.includes='ParallelProcessingBenchmark\.(raw|kpipe|atleonParallel|confluent|reactor|share|kpipeKeyOrdered|atleonKey|confluentKey)$' \
  -Pjmh.workMicros=0,100,1000 -Pjmh.fork=3 -Pjmh.iterations=5 -Pjmh.warmupIterations=3 \
  -Pjmh.profilers=gc -Pjmh.resultFormat=JSON
```

## Throughput — unordered arms

Records per second, higher is better, with the JMH 99.9% error.

| Runtime                                 |     workMicros=0 |   workMicros=100 |  workMicros=1000 |
| --------------------------------------- | ---------------: | ---------------: | ---------------: |
| Raw `KafkaConsumer` + virtual threads   | 901,585 ± 98,350 | 617,337 ± 49,605 | 626,174 ± 45,390 |
| KPipe `PARALLEL`                        | 814,128 ± 86,939 | 533,108 ± 44,482 | 515,962 ± 46,702 |
| Atleon parallel                         | 561,443 ± 28,782 | 467,172 ± 23,918 | 460,556 ± 26,307 |
| Confluent Parallel Consumer `UNORDERED` |  115,508 ± 2,521 |  110,835 ± 3,150 |     70,987 ± 814 |
| Reactor Kafka                           | 436,324 ± 14,551 |      23,442 ± 88 |        3,722 ± 3 |
| Kafka Share Consumer                    |       4,738 ± 25 |       4,707 ± 33 |       4,689 ± 34 |

## Throughput — per-key ordered arms

Compare these with each other. Per-key ordering is a stronger guarantee than the unordered arms give.

| Runtime                           |     workMicros=0 |   workMicros=100 |  workMicros=1000 |
| --------------------------------- | ---------------: | ---------------: | ---------------: |
| KPipe `KEY_ORDERED`               | 749,470 ± 85,943 | 522,978 ± 40,948 | 324,964 ± 12,937 |
| Atleon key-ordered                | 670,397 ± 32,729 | 492,708 ± 28,519 | 352,423 ± 11,914 |
| Confluent Parallel Consumer `KEY` |  101,823 ± 2,806 |   99,379 ± 2,427 |     63,757 ± 906 |

## Ratios within this run

| Ratio                                    | workMicros=0 | workMicros=100 | workMicros=1000 |
| ---------------------------------------- | -----------: | -------------: | --------------: |
| KPipe `PARALLEL` ÷ Atleon parallel       |         1.45 |           1.14 |            1.12 |
| KPipe `PARALLEL` ÷ raw loop              |         0.90 |           0.86 |            0.82 |
| Atleon parallel ÷ raw loop               |         0.62 |           0.76 |            0.74 |
| KPipe `KEY_ORDERED` ÷ Atleon key-ordered |         1.12 |           1.06 |            0.92 |
| KPipe `KEY_ORDERED` ÷ Confluent PC `KEY` |         7.36 |           5.26 |            5.10 |

### Observations

- KPipe `PARALLEL` leads Atleon parallel at every work level. The error bands are apart at 0 µs and overlap at 100 and
  1000 µs, where the lead is 12 to 14%.
- Atleon key-ordered leads KPipe `KEY_ORDERED` at 1000 µs, 352k ± 12k against 325k ± 13k, and the error bands are apart.
  At 0 and 100 µs KPipe is ahead, but the error bands overlap.
- The raw loop is still the fastest unordered arm. It tracks no offsets and makes no delivery guarantee, so it is a
  ceiling rather than a competitor.
- Before #357, KPipe paused for a full 100 ms poll timeout each time in-flight backpressure engaged, which held it to
  about 0.21x to 0.24x of the raw loop at 100 and 1000 µs on the same kind of runner. That is the main change from
  earlier captures.
- The Share Consumer arm stays near 4.7k records per second at every work level, as in earlier captures.

## Batch drain time — ParallelProcessingLatencyBenchmark

Milliseconds to drain one batch of 25,000 records, lower is better. JMH reports these as amortized milliseconds per
record because every arm uses `@OperationsPerInvocation`; the values below multiply them back by 25,000. The samples
describe whole batches, not the latency of individual records, which `PerRecordLatencyHarness` measures.

Captured in [run 38741641109](https://github.com/eschizoid/kpipe/actions/runs/38741641109) on `main` at `c4a660d7`, on
the same kind of runner as the throughput capture, with 2 forks, 2 warmup and 4 measurement iterations. The first
attempt at 3 forks and 5 iterations hit the 180-minute job limit. Raw output:
[`2026-10-15-ci-latency.json`](2026-10-15-ci-latency.json).

### workMicros=0

| Runtime                                 |     Average |   p50 |   p95 |   p99 |
| --------------------------------------- | ----------: | ----: | ----: | ----: |
| Raw `KafkaConsumer` + virtual threads   |  29.6 ± 5.5 |  28.6 |  39.4 |  50.2 |
| KPipe `PARALLEL`                        |  31.1 ± 6.3 |  31.6 |  45.6 |  53.3 |
| Atleon key-ordered                      |  36.4 ± 5.9 |  33.3 |  58.5 |  76.8 |
| Atleon parallel                         |  42.9 ± 5.7 |  40.8 |  64.2 |  83.1 |
| Reactor Kafka                           |  54.0 ± 3.4 |  54.3 |  80.9 |  92.3 |
| Atleon per-partition                    |  75.8 ± 8.7 |  38.9 | 227.6 | 334.8 |
| Confluent Parallel Consumer `UNORDERED` | 210.7 ± 7.2 | 212.8 | 233.5 | 256.6 |

### workMicros=100

| Runtime                                 |        Average |     p50 |     p95 |     p99 |
| --------------------------------------- | -------------: | ------: | ------: | ------: |
| Raw `KafkaConsumer` + virtual threads   |     42.4 ± 8.9 |    43.8 |    59.5 |    69.4 |
| KPipe `PARALLEL`                        |     42.5 ± 6.1 |    43.6 |    54.8 |    60.2 |
| Atleon key-ordered                      |     50.6 ± 9.3 |    47.1 |    69.3 |    88.4 |
| Atleon parallel                         |     51.8 ± 6.6 |    48.9 |    72.0 |    86.9 |
| Confluent Parallel Consumer `UNORDERED` |    221.0 ± 7.8 |   217.4 |   237.3 |   249.3 |
| Atleon per-partition                    | 1,027.0 ± 16.4 | 1,019.2 | 1,044.5 | 1,113.6 |
| Reactor Kafka                           | 1,058.8 ± 11.9 | 1,057.6 | 1,078.2 | 1,081.6 |

### workMicros=1000

| Runtime                                 |        Average |     p50 |     p95 |     p99 |
| --------------------------------------- | -------------: | ------: | ------: | ------: |
| Raw `KafkaConsumer` + virtual threads   |     42.5 ± 4.8 |    42.0 |    59.3 |    64.1 |
| KPipe `PARALLEL`                        |     45.4 ± 6.1 |    46.3 |    61.0 |    69.3 |
| Atleon parallel                         |     51.8 ± 5.2 |    49.7 |    70.8 |    82.5 |
| Atleon key-ordered                      |     70.4 ± 3.6 |    68.9 |    86.7 |   102.7 |
| Confluent Parallel Consumer `UNORDERED` |    346.3 ± 4.8 |   345.2 |   364.3 |   371.4 |
| Atleon per-partition                    | 6,100.8 ± 23.9 | 6,105.6 | 6,195.2 | 6,195.2 |
| Reactor Kafka                           | 6,698.4 ± 24.5 | 6,681.6 | 6,681.6 | 6,681.6 |

### Observations

- KPipe `PARALLEL` drains a batch faster than Atleon parallel at every work level, and its p99 is 13 to 30 ms lower. The
  error bands of the averages overlap at all three levels, narrowly at 0 µs.
- KPipe's average stays within about 3 ms of the raw loop at every work level.
- Reactor Kafka and Atleon per-partition take about 1 second per batch at 100 µs and 6 to 7 seconds at 1000 µs. Atleon
  per-partition processes each partition's records in order. Reactor runs `Flux.parallel(100)` with no ordering, and
  this capture does not show why it slows down as the work grows.

### Per-key ordered arms

The per-key arms ran together in a separate job,
[run 38763745698](https://github.com/eschizoid/kpipe/actions/runs/38763745698), on the same kind of runner and with the
same settings, after `kpipeKeyOrdered` and `confluentKey` were added to the latency benchmark. Raw output:
[`2026-10-15-ci-latency-key.json`](2026-10-15-ci-latency-key.json).

#### workMicros=0

| Runtime                           |      Average |   p50 |   p95 |   p99 |
| --------------------------------- | -----------: | ----: | ----: | ----: |
| KPipe `KEY_ORDERED`               |   34.1 ± 6.2 |  35.3 |  49.0 |  51.7 |
| Atleon key-ordered                |   35.3 ± 8.2 |  31.7 |  60.5 | 108.5 |
| Confluent Parallel Consumer `KEY` | 237.9 ± 10.7 | 242.2 | 267.0 | 290.7 |

#### workMicros=100

| Runtime                           |      Average |   p50 |   p95 |   p99 |
| --------------------------------- | -----------: | ----: | ----: | ----: |
| KPipe `KEY_ORDERED`               |   47.8 ± 6.6 |  46.5 |  63.7 |  71.1 |
| Atleon key-ordered                |  52.4 ± 10.0 |  47.5 |  73.9 | 109.4 |
| Confluent Parallel Consumer `KEY` | 278.8 ± 14.3 | 281.6 | 318.0 | 324.1 |

#### workMicros=1000

| Runtime                           |     Average |   p50 |   p95 |   p99 |
| --------------------------------- | ----------: | ----: | ----: | ----: |
| Atleon key-ordered                |  74.8 ± 5.2 |  72.6 |  93.7 | 114.0 |
| KPipe `KEY_ORDERED`               |  83.1 ± 8.7 |  81.1 | 100.7 | 110.2 |
| Confluent Parallel Consumer `KEY` | 396.5 ± 9.6 | 400.0 | 428.8 | 436.8 |

- The averages of KPipe `KEY_ORDERED` and Atleon key-ordered are level at 0 and 100 µs. At 1000 µs Atleon is faster on
  average, 74.8 ms against 83.1 ms, and the error bands overlap slightly.
- KPipe's p99 is about half of Atleon's at 0 and 100 µs, 51.7 ms against 108.5 ms and 71.1 ms against 109.4 ms. At 1000
  µs the two are level.
- Confluent `KEY` takes 5 to 7 times as long per batch as either.

## Allocation (gc profiler)

Only the two KPipe figures here are close to correct, within 7%, and the table should not be used to compare libraries.
JMH 1.36's `gc` profiler sums per-thread allocation counters over the threads alive at each snapshot, so it drops
everything allocated by a thread that exits during the iteration. The other arms run their consumers on threads that are
created and stopped on every invocation, so their allocation was under-counted, while KPipe's virtual-thread poll loop
was counted fully. The [2026-10-16 capture](2026-10-16-ci-parallel-jmh137.md) repeats the measurement with JMH 1.37,
which counts every thread. There, KPipe `PARALLEL` allocates about 1,044 bytes per record, against 1,001 for the raw
loop and 1,701 for Atleon parallel.

`gc.alloc.rate.norm` as JMH 1.36 reported it, bytes per record at workMicros=0:

| Runtime                                 | B/record |
| --------------------------------------- | -------: |
| Kafka Share Consumer                    |       34 |
| Confluent Parallel Consumer `KEY`       |       52 |
| Raw `KafkaConsumer` + virtual threads   |       53 |
| Confluent Parallel Consumer `UNORDERED` |       57 |
| Reactor Kafka                           |      124 |
| Atleon parallel                         |      311 |
| Atleon key-ordered                      |      621 |
| KPipe `KEY_ORDERED`                     |      964 |
| KPipe `PARALLEL`                        |    1,046 |

## Pitfalls in this run

- A shared CI runner is noisier than a dedicated machine. Read the ratios, which compare arms on the same machine in the
  same run, more than the absolute numbers.
- The 2026-10-12 capture ran on an Apple M3 Max and mixed in a separate Atleon rerun, so its absolute numbers and its
  cross-arm ratios are not comparable with this one.

## Raw output

[`2026-10-15-ci-parallel.json`](2026-10-15-ci-parallel.json) is the full JMH output, including the `gc` profiler
metrics.
