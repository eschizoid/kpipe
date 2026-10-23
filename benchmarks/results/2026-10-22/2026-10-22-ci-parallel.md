# Benchmark snapshot — 2026-10-22 CI parallel-processing capture

`ParallelProcessingBenchmark` for the nine arms in the comparison graph, with five forks, JMH 1.37, and KPipe committing
offsets. This is the capture behind the comparison graph and the README figures. Every ratio below compares arms from
the same run.

## What changed since the 2026-10-21 capture

- KPipe commits offsets. Before #366, a consumer built without an `OffsetManager` never tracked or committed offsets, so
  the benchmark's KPipe arms did neither, while the Atleon arms acknowledged every record. Every KPipe consumer now
  tracks each record and commits the frontier through a `KafkaOffsetManager`.
- Reactor runs fairly. Its 100 rails ran on `Schedulers.parallel()`, one thread per CPU, so its blocking work ran four
  records at a time on this runner. #367 moved them to a virtual-thread scheduler, as the Atleon arms use.
- Five forks instead of three, because #365 raised the job's limit to 360 minutes.

## Environment

| Field         | Value                                                                      |
| ------------- | -------------------------------------------------------------------------- |
| Date (UTC)    | 2026-10-22 20:58 → 2026-10-23 00:20                                        |
| Hardware      | GitHub-hosted runner, standard public-repository size (4 vCPU)             |
| OS            | Ubuntu 24.04                                                               |
| JDK           | 25.0.4                                                                     |
| JMH           | 1.37                                                                       |
| Kafka broker  | 4.3.0 in a Testcontainers container (`apache/kafka:4.3.0`)                 |
| kafka-clients | 4.3.0, forced for every arm in `benchmarks/build.gradle.kts`               |
| KPipe         | `main` at `fa7a7b9e`                                                       |
| Atleon        | 0.47.1, `AcknowledgementQueueMode.COMPACT_EAGER`                           |
| Workflow run  | [39853348759](https://github.com/eschizoid/kpipe/actions/runs/39853348759) |

JMH ran with 5 forks, 3 warmup iterations, 5 measurement iterations, and the `gc` profiler.

## Allocation (gc profiler)

`gc.alloc.rate.norm`, bytes per record, lower is better.

| Runtime                                 | workMicros=0 | workMicros=100 | workMicros=1000 |
| --------------------------------------- | -----------: | -------------: | --------------: |
| Raw `KafkaConsumer` + virtual threads   |        1,002 |          1,419 |           1,408 |
| KPipe `PARALLEL`                        |        1,128 |          1,820 |           1,773 |
| Atleon parallel                         |        1,682 |          2,246 |           2,270 |
| Confluent Parallel Consumer `UNORDERED` |        3,412 |          3,303 |           3,242 |
| Reactor Kafka                           |        1,029 |            913 |             915 |
| Kafka Share Consumer                    |        1,401 |          1,875 |           1,882 |
| KPipe `KEY_ORDERED`                     |        1,063 |            969 |             985 |
| Atleon key-ordered                      |        1,357 |          1,423 |           1,408 |
| Confluent Parallel Consumer `KEY`       |        3,674 |          3,657 |           3,495 |

KPipe `PARALLEL` allocates 13% more per record than the raw loop at 0 µs and 26 to 28% more at 100 and 1000 µs. Against
the 2026-10-21 capture, which ran without offset tracking, its allocation rose 8%, 14% and 7%, and KPipe `KEY_ORDERED`'s
rose 4 to 9%. At every delay, each KPipe mode still allocates less than the Atleon arm with the same ordering (0.67 to
0.81 of it) and less than Confluent Parallel Consumer (0.26 to 0.55 of it).

## Throughput — unordered arms

Records per second, higher is better, with the JMH 99.9% error.

| Runtime                                 |       workMicros=0 |   workMicros=100 |  workMicros=1000 |
| --------------------------------------- | -----------------: | ---------------: | ---------------: |
| Raw `KafkaConsumer` + virtual threads   | 1,000,774 ± 84,986 | 707,411 ± 54,598 | 685,000 ± 47,420 |
| KPipe `PARALLEL`                        |   713,023 ± 58,674 | 516,110 ± 72,481 | 543,938 ± 38,066 |
| Atleon parallel                         |   656,208 ± 25,844 | 542,580 ± 19,095 | 522,302 ± 26,758 |
| Confluent Parallel Consumer `UNORDERED` |    116,802 ± 1,959 |   96,319 ± 2,100 |     69,509 ± 547 |
| Reactor Kafka                           |   863,386 ± 36,916 |  347,842 ± 7,516 |     84,739 ± 648 |
| Kafka Share Consumer                    |         4,748 ± 17 |       4,726 ± 20 |       4,680 ± 26 |

## Throughput — per-key ordered arms

| Runtime                           |     workMicros=0 |   workMicros=100 |  workMicros=1000 |
| --------------------------------- | ---------------: | ---------------: | ---------------: |
| KPipe `KEY_ORDERED`               | 657,777 ± 37,616 | 503,423 ± 34,927 | 315,963 ± 11,676 |
| Atleon key-ordered                | 778,233 ± 32,777 | 496,457 ± 20,356 |  334,719 ± 9,761 |
| Confluent Parallel Consumer `KEY` |  105,861 ± 2,184 |   83,130 ± 1,604 |   62,247 ± 1,498 |

## Ratios within this run

"Apart" means the two arms' error bands do not overlap.

| Ratio                                    | workMicros=0 | workMicros=100 | workMicros=1000 |
| ---------------------------------------- | -----------: | -------------: | --------------: |
| KPipe `PARALLEL` ÷ Atleon parallel       |         1.09 |           0.95 |            1.04 |
| Error bands                              |      overlap |        overlap |         overlap |
| KPipe `KEY_ORDERED` ÷ Atleon key-ordered |         0.85 |           1.01 |            0.94 |
| Error bands                              |        apart |        overlap |         overlap |
| KPipe `PARALLEL` ÷ raw loop              |         0.71 |           0.73 |            0.79 |
| Error bands                              |        apart |          apart |           apart |
| KPipe `PARALLEL` ÷ Confluent `UNORDERED` |         6.10 |           5.36 |            7.83 |
| KPipe `KEY_ORDERED` ÷ Confluent `KEY`    |         6.21 |           6.06 |            5.08 |
| Reactor ÷ KPipe `PARALLEL`               |         1.21 |           0.67 |            0.16 |
| Error bands                              |        apart |          apart |           apart |

## What the offset tracking costs

Against the 2026-10-21 capture, KPipe `PARALLEL`'s throughput fell 22%, 22% and 12% at the three delays, and KPipe
`KEY_ORDERED`'s fell 18%, 17% and 3%. The raw loop and Atleon parallel, which did not change between the two runs, fell
4 to 6% at 0 and 100 µs, but Confluent `KEY`, which did not change either, fell 11% at 100 µs, so run-to-run variation
on this runner can reach about 11%. At 0 µs, where the two runs' error bands for KPipe `PARALLEL` are apart, tracking
each record costs it roughly 15 to 20%; at 100 µs the two runs' bands overlap, so the change there is not resolved. Each
KPipe invocation drains 25,000 records in about 35 to 80 ms, well inside its 30-second commit interval, and the commit
at shutdown runs in the untimed teardown, so this is the cost of tracking, not of committing. Atleon and Reactor also
commit outside the timed window, after a first 5-second delay. Confluent Parallel Consumer commits once as soon as its
first record completes, inside each invocation, and the share consumer acknowledges on every poll. With commits on,
KPipe and Atleon, which acknowledges every record, are within each other's error bands at every delay in unordered mode.
In key-ordered mode Atleon leads at 0 µs and the two are within the error bands at 100 and 1000 µs.

## Reactor

With its rails on a virtual-thread scheduler, Reactor ran at 2.0, 15.4 and 22.7 times its 2026-10-21 throughput. It is
the fastest framework arm at 0 µs, at 1.21 times KPipe, and falls behind once records carry work, at 0.67 and 0.16 times
KPipe at 100 and 1000 µs: its 100 rails process at most 100 records at a time, while KPipe runs one virtual thread per
record up to its backpressure watermark. Reactor also commits the latest acknowledged offset per partition
(`maxDeferredCommits` is 0), not a lowest-pending frontier, which is cheaper than KPipe's and Atleon's tracking and does
not hold at-least-once when records finish out of order.

## Raw output

[`2026-10-22-ci-parallel.json`](2026-10-22-ci-parallel.json) is the full JMH output, including the `gc` profiler
metrics.
