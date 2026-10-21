# Benchmark snapshot — 2026-10-16 CI capture with Atleon `COMPACT_EAGER`

`ParallelProcessingBenchmark` for KPipe, Atleon and the raw loop after Atleon's receivers were switched to
`AcknowledgementQueueMode.COMPACT_EAGER` in #361. Every ratio below compares arms from the same run.

## Environment

| Field        | Value                                                                      |
| ------------ | -------------------------------------------------------------------------- |
| Date (UTC)   | 2026-10-16 08:54 → 10:23                                                   |
| Hardware     | GitHub-hosted runner, standard public-repository size (4 vCPU)             |
| OS           | Ubuntu 24.04                                                               |
| JDK          | 25.0.4                                                                     |
| JMH          | 1.36                                                                       |
| Kafka        | 4.3.0 in a Testcontainers container (`apache/kafka:4.3.0`)                 |
| KPipe        | `main` at `f30657cf` plus the change merged in #361                        |
| Atleon       | 0.47.1, `AcknowledgementQueueMode.COMPACT_EAGER`                           |
| Workflow run | [38876804488](https://github.com/eschizoid/kpipe/actions/runs/38876804488) |

JMH ran with 3 forks, 3 warmup iterations and 5 measurement iterations. The run used JMH 1.36, whose `gc` profiler
under-counts allocation for every arm except KPipe (see the
[2026-10-16 JMH 1.37 capture](2026-10-16-ci-parallel-jmh137.md)), so this summary reports throughput only.

## Throughput

Records per second, higher is better, with the JMH 99.9% error.

| Runtime                               |      workMicros=0 |   workMicros=100 |  workMicros=1000 | workMicros=10000 |
| ------------------------------------- | ----------------: | ---------------: | ---------------: | ---------------: |
| Raw `KafkaConsumer` + virtual threads | 958,973 ± 114,175 | 697,147 ± 56,196 | 695,366 ± 68,045 | 584,822 ± 64,759 |
| KPipe `PARALLEL`                      |  834,161 ± 92,766 | 613,641 ± 78,282 | 569,999 ± 52,944 | 367,781 ± 20,963 |
| Atleon parallel                       |  691,031 ± 27,726 | 562,687 ± 29,816 | 548,943 ± 31,144 | 398,688 ± 19,849 |
| KPipe `KEY_ORDERED`                   |  755,325 ± 76,680 | 572,305 ± 59,859 | 316,296 ± 16,332 |     50,157 ± 439 |
| Atleon key-ordered                    |  765,404 ± 47,800 | 516,210 ± 28,222 | 336,420 ± 12,923 |     54,266 ± 338 |

## Ratios within this run

| Ratio                                    | workMicros=0 | workMicros=100 | workMicros=1000 | workMicros=10000 |
| ---------------------------------------- | -----------: | -------------: | --------------: | ---------------: |
| KPipe `PARALLEL` ÷ Atleon parallel       |         1.21 |           1.09 |            1.04 |             0.92 |
| Error bands                              |        apart |        overlap |         overlap |          overlap |
| KPipe `KEY_ORDERED` ÷ Atleon key-ordered |         0.99 |           1.11 |            0.94 |             0.92 |
| Error bands                              |      overlap |        overlap |         overlap |            apart |
| KPipe `PARALLEL` ÷ raw loop              |         0.87 |           0.88 |            0.82 |             0.63 |

The 2026-10-15 M3 Max capture used the same `COMPACT_EAGER` setting and found Atleon parallel at 1.86× and 2.05× KPipe
at 100 µs and 1 ms. On this runner the two are within each other's error bands at those delays, so the Atleon setting
does not explain the difference between the two machines.

## Raw output

[`2026-10-16-ci-compact-eager.json`](2026-10-16-ci-compact-eager.json) is the full JMH output.
