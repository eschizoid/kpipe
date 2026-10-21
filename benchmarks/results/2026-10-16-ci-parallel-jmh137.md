# Benchmark snapshot — 2026-10-16 CI parallel-processing capture with JMH 1.37

`ParallelProcessingBenchmark` for the nine arms in the comparison graph, captured with JMH 1.37 so that the `gc`
profiler counts allocation on every thread. Every ratio below compares arms from the same run.

## Why this capture exists

The [2026-10-15 capture](2026-10-15-ci-parallel.md) used JMH 1.36. Its `gc` profiler sums per-thread allocation counters
over the threads that are alive at each snapshot, so it drops everything allocated by a thread that exits during the
iteration. The other arms run their consumers on threads that are created and stopped on every invocation, so their
allocation was under-counted. KPipe polls on a virtual thread, which keeps its allocation on long-lived carrier threads,
so KPipe was counted fully. JMH 1.37 reads the JVM-wide counter instead, which includes virtual threads and threads that
have exited.

## Environment

| Field        | Value                                                                      |
| ------------ | -------------------------------------------------------------------------- |
| Date (UTC)   | 2026-10-16 12:34 → 14:37                                                   |
| Hardware     | GitHub-hosted runner, standard public-repository size (4 vCPU)             |
| OS           | Ubuntu 24.04                                                               |
| JDK          | 25.0.4                                                                     |
| JMH          | 1.37                                                                       |
| Kafka        | 4.3.0 in a Testcontainers container (`apache/kafka:4.3.0`)                 |
| KPipe        | `61e4e611` (main at `f30657cf` plus the JMH 1.37 change)                   |
| Atleon       | 0.47.1, `AcknowledgementQueueMode.STRICT`                                  |
| Workflow run | [38901510755](https://github.com/eschizoid/kpipe/actions/runs/38901510755) |

JMH ran with 3 forks, 3 warmup iterations, 5 measurement iterations, and the `gc` profiler.

## Allocation (gc profiler)

`gc.alloc.rate.norm`, bytes per record at workMicros=0, lower is better. The JMH 1.36 column comes from the 2026-10-15
capture.

| Runtime                                 | JMH 1.36 | JMH 1.37 |
| --------------------------------------- | -------: | -------: |
| Reactor Kafka                           |      124 |      887 |
| Raw `KafkaConsumer` + virtual threads   |       53 |    1,001 |
| KPipe `KEY_ORDERED`                     |      964 |    1,027 |
| KPipe `PARALLEL`                        |    1,046 |    1,044 |
| Atleon key-ordered                      |      621 |    1,465 |
| Kafka Share Consumer                    |       34 |    1,678 |
| Atleon parallel                         |      311 |    1,701 |
| Confluent Parallel Consumer `UNORDERED` |       57 |    3,445 |
| Confluent Parallel Consumer `KEY`       |       52 |    3,742 |

KPipe's figures moved by less than 7% between the two versions, while every other arm rose, some by a factor of 20 or
more. KPipe `PARALLEL` allocates about 4% more per record than the raw loop, and less than Atleon or Confluent Parallel
Consumer.

## Throughput — unordered arms

Records per second, higher is better, with the JMH 99.9% error.

| Runtime                                 |        workMicros=0 |   workMicros=100 |  workMicros=1000 |
| --------------------------------------- | ------------------: | ---------------: | ---------------: |
| Raw `KafkaConsumer` + virtual threads   | 1,069,533 ± 131,623 | 779,943 ± 75,157 | 772,657 ± 73,670 |
| KPipe `PARALLEL`                        |    916,218 ± 87,585 | 658,333 ± 69,152 | 644,134 ± 81,768 |
| Atleon parallel                         |    797,375 ± 47,227 | 645,581 ± 30,372 | 631,364 ± 33,421 |
| Confluent Parallel Consumer `UNORDERED` |     125,994 ± 3,458 |  117,640 ± 2,737 |   73,760 ± 1,094 |
| Reactor Kafka                           |    450,376 ± 14,993 |      23,863 ± 81 |        3,741 ± 8 |
| Kafka Share Consumer                    |          4,752 ± 29 |       4,752 ± 28 |       4,706 ± 37 |

## Throughput — per-key ordered arms

| Runtime                           |     workMicros=0 |   workMicros=100 |  workMicros=1000 |
| --------------------------------- | ---------------: | ---------------: | ---------------: |
| KPipe `KEY_ORDERED`               | 811,986 ± 93,417 | 615,755 ± 73,296 | 332,756 ± 21,550 |
| Atleon key-ordered                | 827,059 ± 47,984 | 578,623 ± 32,275 | 372,290 ± 11,495 |
| Confluent Parallel Consumer `KEY` |  114,018 ± 2,680 |  108,374 ± 1,862 |     67,982 ± 846 |

## Ratios, and how much they move between runs

| Ratio                                              | workMicros=0 | workMicros=100 | workMicros=1000 |
| -------------------------------------------------- | -----------: | -------------: | --------------: |
| KPipe `PARALLEL` ÷ Atleon parallel, this run       |         1.15 |           1.02 |            1.02 |
| KPipe `PARALLEL` ÷ Atleon parallel, 2026-10-15 run |         1.45 |           1.14 |            1.12 |
| KPipe `PARALLEL` ÷ raw loop, this run              |         0.86 |           0.84 |            0.83 |
| KPipe `KEY_ORDERED` ÷ Atleon key-ordered, this run |         0.98 |           1.06 |            0.89 |

The 2026-10-15 run used the same code and the same Atleon settings on the same kind of runner, and differed only in the
JMH version. Its KPipe ÷ Atleon ratios were up to 0.30 higher. Ratios on shared CI runners therefore move between runs,
and a difference smaller than that is not evidence that one library is faster.

## Raw output

[`2026-10-16-ci-parallel-jmh137.json`](2026-10-16-ci-parallel-jmh137.json) is the full JMH output, including the `gc`
profiler metrics.
