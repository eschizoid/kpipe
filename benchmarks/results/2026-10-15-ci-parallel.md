# Benchmark snapshot — 2026-10-15 CI parallel-processing capture

`ParallelProcessingBenchmark` for the nine arms in the comparison graph, captured after the in-flight backpressure fix
in #357. All arms ran in one job on one machine, so every ratio below compares arms from the same run.

## Environment

| Field                       | Value                                                                      |
| --------------------------- | -------------------------------------------------------------------------- |
| Date (UTC)                  | 2026-10-15 08:12 → 10:15                                                   |
| Hardware                    | GitHub-hosted runner, standard public-repository size (4 vCPU)             |
| OS                          | Ubuntu 24.04 (runner image 20261012.334.1)                                 |
| JDK                         | 25.0.4 (Java HotSpot(TM) 64-Bit Server VM)                                 |
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

- KPipe `PARALLEL` leads Atleon parallel at every work level. The error bands are apart at 0 µs and overlap slightly at
  100 and 1000 µs, where the lead is 12 to 14%.
- Atleon key-ordered leads KPipe `KEY_ORDERED` at 1000 µs, 352k ± 12k against 325k ± 13k, and the error bands are apart.
  At 0 and 100 µs KPipe is ahead, but the error bands overlap.
- The raw loop is still the fastest unordered arm. It tracks no offsets and makes no delivery guarantee, so it is a
  ceiling rather than a competitor.
- Before #357, KPipe paused for a full 100 ms poll timeout each time in-flight backpressure engaged, which held it to
  about 0.21x of the raw loop at 100 and 1000 µs on the same kind of runner. That is the main change from earlier
  captures.
- The Share Consumer arm stays near 4.7k records per second at every work level, as in earlier captures.

## Allocation (gc profiler)

`gc.alloc.rate.norm`, bytes per record at workMicros=0, lower is better.

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

KPipe allocates the most per record of any arm. The raw loop also starts a virtual thread per record and allocates 53
bytes, so the thread alone does not explain the difference.

## Pitfalls in this run

- A shared CI runner is noisier than a dedicated machine. Read the ratios, which compare arms on the same machine in the
  same run, more than the absolute numbers.
- The 2026-10-12 capture ran on an Apple M3 Max and mixed in a separate Atleon rerun, so its absolute numbers and its
  cross-arm ratios are not comparable with this one.

## Raw output

[`2026-10-15-ci-parallel.json`](2026-10-15-ci-parallel.json) is the full JMH output, including the `gc` profiler
metrics.
