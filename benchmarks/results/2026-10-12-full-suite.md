# Benchmark snapshot — 2026-10-12-full-suite

All JMH benchmarks with the runtime workload restricted to `workMicros=0,100,1000`: throughput, average-time and
sample-time latency, JSON/Avro design validation, batch sink, MockConsumer dispatch, and offset tracking. The 10–100 ms
workload sweep and the non-JMH `PerRecordLatencyHarness` are outside this capture.

## Environment

| Field                         | Value                                                                                                                                                                                           |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Date                          | 2026-10-12 approximately 10:31 CDT → 2026-10-12 16:14 CDT                                                                                                                                       |
| Resume                        | 2026-10-12 14:33 CDT; 1.69 hours after resume                                                                                                                                                   |
| Hardware                      | Apple M3 Max; 16 physical/logical cores; 64 GiB RAM                                                                                                                                             |
| OS                            | macOS 26.6.2 (build 25G83)                                                                                                                                                                      |
| JDK                           | openjdk version "25.0.2" 2026-01-20 LTS; OpenJDK Runtime Environment Zulu25.32+21-CA (build 25.0.2+10-LTS); OpenJDK 64-Bit Server VM Zulu25.32+21-CA (build 25.0.2+10-LTS, mixed mode, sharing) |
| JMH                           | 1.36                                                                                                                                                                                            |
| Kafka                         | 4.3.1 client / `apache/kafka:4.3.0` broker via Testcontainers                                                                                                                                   |
| Docker                        | 29.4.0; 16 CPUs; approximately 15.7 GiB available to Docker                                                                                                                                     |
| KPipe                         | `650d58f1531da01335978b79e9c74d166dd9de64` plus uncommitted working-tree changes                                                                                                                |
| Atleon                        | 0.47.1                                                                                                                                                                                          |
| Confluent Parallel Consumer   | 0.5.3.3                                                                                                                                                                                         |
| Reactor Kafka                 | 1.3.25                                                                                                                                                                                          |
| Resumed jar SHA-256           | `9845bcfd0e60c2d8143d8bd61ab7c704c5893602e571f388ed5647a32c15f2b5`                                                                                                                              |
| Source diff at resume SHA-256 | `1e59ec1d0f31f15d56a990c3e976919b78a2eb3f3e8a9656319b22e6f74799e8`                                                                                                                              |

The machine was not independently verified to be quiesced. Docker and benchmark forks share host CPU resources. Results
characterize this working tree and machine, rather than a released KPipe artifact.

## JMH configuration

| Parameter       | Value                                                           |
| --------------- | --------------------------------------------------------------- |
| Warmup          | 3 iterations × 2 seconds                                        |
| Measurement     | 5 iterations × 3 seconds                                        |
| Forks / threads | 5 / 1                                                           |
| Profiler        | `gc`                                                            |
| Force GC        | `true`                                                          |
| Result format   | JSON — [2026-10-12-full-suite.json](2026-10-12-full-suite.json) |
| Coverage        | 100/100 current benchmark cases                                 |

Measurements can overrun the requested window while a full invocation completes. Every completed case retains five forks
× five measured iterations; sample-time cases retain their per-iteration histograms. Errors below are relative JMH score
errors, representing its 99.9% confidence intervals.

## Scope and reproduction

Each expanded parameter/mode case ran in a separate JMH invocation, with the common options:

```bash
java -jar <benchmark.jar> "^<benchmark FQN>$" \
  -bm <thrpt|avgt|sample> -wi 3 -w 2s -i 5 -r 3s -f 5 -t 1 \
  -prof gc -gc true -foe true \
  -jvmArgsAppend "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn" \
  -p <parameter>=<value> -rf json -rff <case-output.json>
```

JMH parameters and timing settings are recorded in the raw JSON. JSON/Avro methods have no parameter override; batch
sink sweeps `sinkLatencyMicros=10,100,1000` × `batchSize=1,10,100`; dispatch sweeps `mode=PARALLEL,KEY_ORDERED` ×
`distinctKeys=1,100,10000`; offset tracking uses eight partitions.

Atleon uses `KafkaReceiver.receiveManual` adapted through `AloFlux.wrapper(KafkaReceiverRecord::toAloConsumerRecord)`,
virtual-thread schedulers, and a 25,000-record in-flight limit. Parallel uses `flatMap` with `Integer.MAX_VALUE`
concurrency. Key-ordered uses `groupByWithAutoComplete` with the same maximum concurrency. Partition-ordered uses
`groupBy(ConsumerRecord::partition, Integer.MAX_VALUE)`, `max.poll.records=50`, and greatest-batch-lag polling. Parallel
and key-ordered retain receiver defaults. Grouped arms use default `innerPublishOn` prefetch. KPipe has no
partition-ordered arm. KPipe retains its default in-flight backpressure watermarks (10,000 high / 7,000 low). Confluent
uses `maxConcurrency=100`; partition ordering limits active processing to the eight partitions. Reactor uses 100 rails
on `Schedulers.parallel()`. These settings compare the configured implementations, rather than normalizing all
concurrency and polling controls. The broker topic has eight partitions and 1,000 distinct keys.

## Throughput — ParallelProcessingBenchmark

Records/s, higher is better. JMH normalizes by `@OperationsPerInvocation(25_000)`. `workMicros` is synthetic blocking
delay via `LockSupport.parkNanos`, rather than CPU work or measured external I/O.

| Runtime arm          | Ordering        | 0 µs             | 100 µs           | 1000 µs          |
| -------------------- | --------------- | ---------------- | ---------------- | ---------------- |
| `kpipe`              | Unordered       | 574,267 ± 8.2%   | 235,673 ± 25.0%  | 230,360 ± 22.7%  |
| `atleonParallel`     | Unordered       | 608,344 ± 4.5%   | 851,872 ± 4.2%   | 1,018,327 ± 4.4% |
| `kpipeKeyOrdered`    | Per key         | 384,765 ± 3.9%   | 208,753 ± 20.5%  | 91,925 ± 0.8%    |
| `atleonKey`          | Per key         | 1,657,807 ± 1.5% | 1,186,732 ± 1.3% | 575,752 ± 1.4%   |
| `confluent`          | Unordered       | 104,485 ± 4.8%   | 107,559 ± 4.4%   | 68,111 ± 0.5%    |
| `confluentKey`       | Per key         | 103,644 ± 1.2%   | 101,820 ± 1.1%   | 64,536 ± 0.6%    |
| `confluentPartition` | Per partition   | 1,962 ± 10.3%    | 1,915 ± 9.9%     | 1,401 ± 8.1%     |
| `atleonPartition`    | Per partition   | 1,036,177 ± 6.9% | 32,145 ± 0.5%    | 3,191 ± 6.7%     |
| `reactor`            | Unordered       | 363,691 ± 1.3%   | 99,960 ± 0.3%    | 10,201 ± 6.6%    |
| `raw`                | Unordered       | 547,456 ± 4.7%   | 576,470 ± 4.4%   | 540,629 ± 4.5%   |
| `share`              | Unordered       | 4,806 ± 0.2%     | 4,809 ± 0.1%     | 4,791 ± 0.1%     |
| `singleThread`       | Globally serial | 1,710,374 ± 3.3% | 6,200 ± 1.8%     | 760 ± 5.4%       |

### Matched ordering comparisons

| Ordering      | Work (µs) | Comparison                               | Sample-mean ratio |
| ------------- | --------- | ---------------------------------------- | ----------------- |
| Parallel      | 0         | `atleonParallel` / `kpipe`               | 1.06×             |
| Parallel      | 100       | `atleonParallel` / `kpipe`               | 3.61×             |
| Parallel      | 1000      | `atleonParallel` / `kpipe`               | 4.42×             |
| Per key       | 0         | `atleonKey` / `kpipeKeyOrdered`          | 4.31×             |
| Per key       | 100       | `atleonKey` / `kpipeKeyOrdered`          | 5.68×             |
| Per key       | 1000      | `atleonKey` / `kpipeKeyOrdered`          | 6.26×             |
| Per partition | 0         | `atleonPartition` / `confluentPartition` | 528.11×           |
| Per partition | 100       | `atleonPartition` / `confluentPartition` | 16.78×            |
| Per partition | 1000      | `atleonPartition` / `confluentPartition` | 2.28×             |

Ratios compare means; they are not confidence intervals for the ratio. Compare matching ordering guarantees. The raw
virtual-thread arm lacks equivalent offset/commit guarantees.

### Observations

The zero-work parallel throughput scores have overlapping error bands. Atleon leads KPipe by 3.61×–4.42× with the
simulated blocking delays and by 4.31×–6.26× in the matching per-key throughput cases. KPipe parallel has wide error
bands at nonzero delays, so the exact ratios remain uncertain. These results apply to the configured receiver and
concurrency settings above.

Atleon leads Confluent in partition mode by 16.78× at 100 µs and 2.28× at 1 ms. The 528× no-work ratio suggests
polling/dispatch behavior deserves investigation before drawing a general production conclusion.

## Latency — ParallelProcessingLatencyBenchmark

Values are **batch-amortized milliseconds per record**, rather than individual-record latency percentiles. Each complete
25,000-record batch sample is divided by 25,000; multiply by 25,000 to recover batch-drain time. Average-time and
sample-time values come from separate trials.

| Arm               | Work (µs) | Average ± error (ms/record) | Sample mean ± error (ms/record) | p50      | p95      | p99      |
| ----------------- | --------- | --------------------------- | ------------------------------- | -------- | -------- | -------- |
| `kpipe`           | 0         | 0.001391 ± 6.0%             | 0.001399 ± 4.1%                 | 0.001412 | 0.001707 | 0.002155 |
| `kpipe`           | 100       | 0.004169 ± 17.1%            | 0.004152 ± 16.1%                | 0.005472 | 0.006302 | 0.009880 |
| `kpipe`           | 1000      | 0.004070 ± 24.1%            | 0.004836 ± 17.1%                | 0.005688 | 0.009904 | 0.010397 |
| `atleonParallel`  | 0         | 0.001551 ± 2.8%             | 0.001620 ± 1.4%                 | 0.001585 | 0.002116 | 0.002571 |
| `atleonParallel`  | 100       | 0.001105 ± 4.6%             | 0.001128 ± 2.2%                 | 0.000992 | 0.002048 | 0.002375 |
| `atleonParallel`  | 1000      | 0.000938 ± 4.3%             | 0.000943 ± 1.8%                 | 0.000879 | 0.001640 | 0.002125 |
| `atleonKey`       | 0         | 0.000593 ± 2.5%             | 0.000597 ± 0.9%                 | 0.000575 | 0.000736 | 0.000993 |
| `atleonKey`       | 100       | 0.000812 ± 2.5%             | 0.000866 ± 0.9%                 | 0.000839 | 0.001074 | 0.001351 |
| `atleonKey`       | 1000      | 0.001724 ± 1.4%             | 0.001733 ± 0.7%                 | 0.001712 | 0.001928 | 0.002565 |
| `atleonPartition` | 0         | 0.001047 ± 8.5%             | 0.001037 ± 7.2%                 | 0.000647 | 0.004656 | 0.004822 |
| `atleonPartition` | 100       | 0.031428 ± 1.1%             | 0.031999 ± 1.6%                 | 0.031632 | 0.033786 | 0.042844 |
| `atleonPartition` | 1000      | 0.283571 ± 0.2%             | 0.336876 ± 0.5%                 | 0.337920 | 0.338944 | 0.338944 |
| `confluent`       | 0         | 0.008575 ± 0.8%             | 0.008556 ± 0.8%                 | 0.008544 | 0.008983 | 0.009753 |
| `confluent`       | 100       | 0.008788 ± 0.7%             | 0.008739 ± 0.7%                 | 0.008720 | 0.009120 | 0.009366 |
| `confluent`       | 1000      | 0.014533 ± 0.4%             | 0.016579 ± 0.4%                 | 0.016560 | 0.017056 | 0.017263 |
| `reactor`         | 0         | 0.002645 ± 2.4%             | 0.002628 ± 2.2%                 | 0.002616 | 0.003568 | 0.004058 |
| `reactor`         | 100       | 0.010001 ± 0.4%             | 0.010003 ± 0.4%                 | 0.009952 | 0.010432 | 0.010750 |
| `reactor`         | 1000      | 0.088823 ± 0.1%             | 0.105462 ± 0.3%                 | 0.105472 | 0.106496 | 0.106496 |
| `raw`             | 0         | 0.001470 ± 3.8%             | 0.001404 ± 3.8%                 | 0.001398 | 0.001700 | 0.001874 |
| `raw`             | 100       | 0.001581 ± 2.7%             | 0.001673 ± 3.7%                 | 0.001653 | 0.002087 | 0.002234 |
| `raw`             | 1000      | 0.001817 ± 4.2%             | 0.001837 ± 4.3%                 | 0.001833 | 0.002334 | 0.002647 |

The zero-work parallel average-time trial favors KPipe slightly, reversing the small throughput lead; the blocking-work
advantage for Atleon persists. Separate trials can differ even with narrow individual error bands: Atleon partition at 1
ms measured 0.283571 ms/record in average-time mode and 0.336876 in sample-time mode. Treat within-trial confidence
intervals separately from across-trial reproducibility.

## Allocation profile

Profiler-reported `gc.alloc.rate.norm` in B/record at `workMicros=100`, from throughput trials. Fixture allocations can
be included even when setup/teardown is excluded from the benchmark timer. These counts do not isolate the processing
path or establish the cause of performance differences.

| Arm                  | B/record |
| -------------------- | -------- |
| `kpipe`              | 1,856.5  |
| `atleonParallel`     | 1,036.3  |
| `kpipeKeyOrdered`    | 908.1    |
| `atleonKey`          | 667.2    |
| `confluent`          | 120.0    |
| `confluentKey`       | 213.8    |
| `confluentPartition` | 163.7    |
| `atleonPartition`    | 684.3    |
| `reactor`            | 155.5    |
| `raw`                | 448.2    |
| `share`              | 424.8    |
| `singleThread`       | 60.6     |

## Design validation

In-memory throughput (ops/s). JSON includes a deliberately inefficient repeated-SerDe baseline; Avro compares plain
serialization and wire-prefix handling paths.

| Benchmark                                     | Throughput ± error | B/op    |
| --------------------------------------------- | ------------------ | ------- |
| JsonPipelineBenchmark.kpipeJsonPipeline       | 1,808,471 ± 0.7%   | 1,800.0 |
| JsonPipelineBenchmark.manualJsonSingleSerDe   | 2,256,254 ± 1.3%   | 1,800.0 |
| JsonPipelineBenchmark.manualJsonSerDeChained  | 726,419 ± 0.7%     | 5,472.0 |
| AvroPipelineBenchmark.kpipeAvroMagicPipeline  | 1,840,538 ± 0.5%   | 3,816.0 |
| AvroPipelineBenchmark.kpipeAvroPipeline       | 1,932,860 ± 0.5%   | 3,760.0 |
| AvroPipelineBenchmark.manualAvroMagicHandling | 2,224,558 ± 0.3%   | 3,816.0 |

## Batch sink

KPipe-only MockConsumer throughput in records/s; each invocation processes 500 records.

| batchSize | sinkLatencyMicros | Throughput ± error |
| --------- | ----------------- | ------------------ |
| 1         | 10                | 52,070 ± 0.2%      |
| 10        | 10                | 446,769 ± 2.8%     |
| 100       | 10                | 1,922,575 ± 0.6%   |
| 1         | 100               | 7,511 ± 0.2%       |
| 10        | 100               | 72,665 ± 0.2%      |
| 100       | 100               | 589,132 ± 0.3%     |
| 1         | 1000              | 752 ± 1.1%         |
| 10        | 1000              | 7,475 ± 1.4%       |
| 100       | 1000              | 68,976 ± 2.3%      |

## Key-ordered dispatch

KPipe-only MockConsumer throughput in records/s; each invocation processes 10,000 no-op records in one partition.
Allocation is profiler-reported B/record, including fixture allocations. These scores come from the captured fixture;
the code now uses the restored original KPipe fixture.

| Mode          | Keys  | Throughput ± error | B/record |
| ------------- | ----- | ------------------ | -------- |
| `PARALLEL`    | 1     | 1,323,276 ± 0.7%   | 1,111.0  |
| `PARALLEL`    | 100   | 1,342,831 ± 0.6%   | 1,125.8  |
| `PARALLEL`    | 10000 | 1,343,455 ± 0.6%   | 1,128.3  |
| `KEY_ORDERED` | 1     | 1,614,567 ± 0.9%   | 741.5    |
| `KEY_ORDERED` | 100   | 680,519 ± 2.9%     | 1,138.7  |
| `KEY_ORDERED` | 10000 | 721,267 ± 3.2%     | 1,349.4  |

## Offset tracking

| Partitions | Throughput ± error (records/s) | B/record |
| ---------- | ------------------------------ | -------- |
| 8          | 18,167,526 ± 0.4%              | 232.0    |

## Raw output and completeness

[2026-10-12-full-suite.json](2026-10-12-full-suite.json) contains unchanged native JMH result objects, including
secondary GC metrics, per-fork samples, and sample-time histograms. No samples within retained cases were discarded or
scores recomputed.

The capture completed 106 cases without failures. This report and JSON retain the 100 cases matching the current
benchmark set; the six removed Atleon mock-consumer cases are saved with their implementation in a Git stash.

### Capture interruption

After case 65, the build directory was removed at approximately 14:29 CDT. The original runner, per-case logs, metadata,
and jar were deleted, while all 65 completed native result objects remained intact in the results directory. The user
confirmed no relevant benchmark or consumer code changes and authorized resuming from case 66. The JMH executable was
rebuilt and copied outside the build directory. This report combines the original 65 cases with the resumed cases using
the same timing, fork, and profiling settings.

Resumed commands, logs, source diff, executable, and metadata are retained locally under
`/private/tmp/kpipe-full-suite-2026-10-12-recovery/`. The jar and source-diff checksums above apply to the resumed
segment; the original executable checksum and logs are unavailable.

## Pitfalls in this run

- Completion measures processing callbacks, excluding final offset commits and some acknowledgment cleanup. These
  measurements do not establish end-to-end delivery latency.
- Atleon scheduler-disposal races and Confluent shutdown interruptions were observed in logs. Cleanup and logging can
  add noise; original-segment log counts are unavailable after the interruption.
- Cases ran sequentially in a fixed order. Cross-JVM variation, background activity, and thermal or run-order effects
  can influence comparisons; five forks do not eliminate those effects.
- The low-work partition gap can reflect receiver polling and dispatch behavior. It should not be generalized into a
  production performance ratio without further experiments.
- macOS supplies no Linux `perf`/CPI measurements here; no hardware-counter conclusions are made.

1 completed case(s) have relative primary score error above 25%; treat those means as especially uncertain. The raw JSON
retains every confidence interval.
