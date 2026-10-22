# Benchmark snapshot — 2026-10-21 CI parallel-processing capture

`ParallelProcessingBenchmark` for the nine arms in the comparison graph, with JMH 1.37 and Atleon's receivers in
`AcknowledgementQueueMode.COMPACT_EAGER`. This is the capture behind the comparison graph and the README figures. Every
ratio below compares arms from the same run.

## Environment

| Field         | Value                                                                      |
| ------------- | -------------------------------------------------------------------------- |
| Date (UTC)    | 2026-10-21 21:37 → 23:40                                                   |
| Hardware      | GitHub-hosted runner, standard public-repository size (4 vCPU)             |
| OS            | Ubuntu 24.04                                                               |
| JDK           | 25.0.4                                                                     |
| JMH           | 1.37                                                                       |
| Kafka broker  | 4.3.0 in a Testcontainers container (`apache/kafka:4.3.0`)                 |
| kafka-clients | 4.3.0, forced for every arm in `benchmarks/build.gradle.kts`               |
| KPipe         | Commit `fbdc0612` on the #364 branch                                       |
| Atleon        | 0.47.1, `AcknowledgementQueueMode.COMPACT_EAGER`                           |
| Workflow run  | [39680348208](https://github.com/eschizoid/kpipe/actions/runs/39680348208) |

JMH ran with 3 forks, 3 warmup iterations, 5 measurement iterations, and the `gc` profiler.

The libraries build against kafka-clients 4.4.0, but Parallel Consumer 0.5.3.3 processes no records on that version:
both Confluent arms timed out with none of the 25,000 records processed in every fork of an earlier run of the same
sweep ([39644170654](https://github.com/eschizoid/kpipe/actions/runs/39644170654)). The benchmarks module therefore
forces 4.3.0 for every arm.

## Allocation (gc profiler)

`gc.alloc.rate.norm`, bytes per record, lower is better.

| Runtime                                 | workMicros=0 | workMicros=100 | workMicros=1000 |
| --------------------------------------- | -----------: | -------------: | --------------: |
| Raw `KafkaConsumer` + virtual threads   |        1,002 |          1,424 |           1,410 |
| KPipe `PARALLEL`                        |        1,046 |          1,591 |           1,651 |
| Atleon parallel                         |        1,683 |          2,289 |           2,244 |
| Confluent Parallel Consumer `UNORDERED` |        3,421 |          3,308 |           3,250 |
| Reactor Kafka                           |          882 |            801 |             828 |
| Kafka Share Consumer                    |        1,386 |          1,861 |           1,843 |
| KPipe `KEY_ORDERED`                     |        1,024 |            905 |             903 |
| Atleon key-ordered                      |        1,355 |          1,422 |           1,408 |
| Confluent Parallel Consumer `KEY`       |        3,540 |          3,620 |           3,531 |

KPipe `PARALLEL` allocates 4% more per record than the raw loop at 0 µs, 12% more at 100 µs and 17% more at 1000 µs. At
every delay, each KPipe mode allocates less than the Atleon arm with the same ordering and less than Confluent Parallel
Consumer: KPipe `PARALLEL` allocates 0.31 to 0.51 times what Confluent `UNORDERED` does, and KPipe `KEY_ORDERED` 0.25 to
0.29 times what Confluent `KEY` does.

## Throughput — unordered arms

Records per second, higher is better, with the JMH 99.9% error.

| Runtime                                 |        workMicros=0 |   workMicros=100 |  workMicros=1000 |
| --------------------------------------- | ------------------: | ---------------: | ---------------: |
| Raw `KafkaConsumer` + virtual threads   | 1,040,106 ± 112,541 | 747,782 ± 62,503 | 747,731 ± 69,335 |
| KPipe `PARALLEL`                        |    914,149 ± 91,611 | 665,097 ± 88,180 | 614,762 ± 56,989 |
| Atleon parallel                         |    695,584 ± 27,640 | 577,294 ± 28,407 | 570,604 ± 25,770 |
| Confluent Parallel Consumer `UNORDERED` |     118,599 ± 3,040 |  100,385 ± 2,193 |     70,650 ± 999 |
| Reactor Kafka                           |    427,481 ± 12,943 |      22,529 ± 97 |        3,726 ± 3 |
| Kafka Share Consumer                    |          4,750 ± 23 |       4,734 ± 18 |       4,688 ± 34 |

## Throughput — per-key ordered arms

| Runtime                           |     workMicros=0 |   workMicros=100 |  workMicros=1000 |
| --------------------------------- | ---------------: | ---------------: | ---------------: |
| KPipe `KEY_ORDERED`               | 804,070 ± 91,804 | 603,068 ± 52,097 | 326,253 ± 17,123 |
| Atleon key-ordered                | 796,689 ± 51,311 | 532,096 ± 23,674 | 345,143 ± 13,440 |
| Confluent Parallel Consumer `KEY` |  108,163 ± 1,856 |   93,167 ± 1,440 |     64,948 ± 804 |

## Ratios within this run

"Apart" means the two arms' error bands do not overlap.

| Ratio                                    | workMicros=0 | workMicros=100 | workMicros=1000 |
| ---------------------------------------- | -----------: | -------------: | --------------: |
| KPipe `PARALLEL` ÷ Atleon parallel       |         1.31 |           1.15 |            1.08 |
| Error bands                              |        apart |        overlap |         overlap |
| KPipe `KEY_ORDERED` ÷ Atleon key-ordered |         1.01 |           1.13 |            0.95 |
| Error bands                              |      overlap |        overlap |         overlap |
| KPipe `PARALLEL` ÷ raw loop              |         0.88 |           0.89 |            0.82 |
| Error bands                              |      overlap |        overlap |           apart |
| KPipe `PARALLEL` ÷ Confluent `UNORDERED` |         7.71 |           6.63 |            8.70 |
| KPipe `KEY_ORDERED` ÷ Confluent `KEY`    |         7.43 |           6.47 |            5.02 |

The [2026-10-16 `COMPACT_EAGER` capture](../2026-10-16/2026-10-16-ci-compact-eager.md), which used JMH 1.36 and
kafka-clients 4.3.1, gave KPipe `PARALLEL` ÷ Atleon parallel of 1.21, 1.09 and 1.04 and KPipe `KEY_ORDERED` ÷ Atleon
key-ordered of 0.99, 1.11 and 0.94 at the same three delays. At these three delays, both runs put KPipe `PARALLEL` ahead
of Atleon parallel at 0 µs with the error bands apart, and every other KPipe ÷ Atleon comparison within the error bands.

## Raw output

[`2026-10-21-ci-parallel.json`](2026-10-21-ci-parallel.json) is the full JMH output, including the `gc` profiler
metrics.
