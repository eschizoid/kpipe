# KPipe versus Atleon throughput — M3 Max capture (2026-10-15)

Full JMH throughput comparison of parallel/unordered and per-key processing at 0 µs, 100 µs, 1 ms, 10 ms, and 100 ms.
The raw virtual-thread consumer and Kafka Share Consumer provide additional unordered baselines. This is a new capture
with KPipe’s backpressure-release fix and Atleon explicitly configured with `AcknowledgementQueueMode.COMPACT_EAGER`; it
does not reuse results from earlier reports.

## Environment

| Field                     | Value                                                                                                                                                                                           |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Matched-library phase     | 2026-10-15 10:12:14 CDT → 2026-10-15 11:07:16 CDT                                                                                                                                               |
| Raw/share baseline phase  | 2026-10-15 11:08:54 CDT → 2026-10-15 11:46:04 CDT                                                                                                                                               |
| Hardware                  | Apple M3 Max; 16 physical/logical cores; 64 GiB RAM                                                                                                                                             |
| OS                        | ProductName: macOS; ProductVersion: 26.6.2; BuildVersion: 25G83                                                                                                                                 |
| JDK                       | openjdk version "25.0.2" 2026-01-20 LTS; OpenJDK Runtime Environment Zulu25.32+21-CA (build 25.0.2+10-LTS); OpenJDK 64-Bit Server VM Zulu25.32+21-CA (build 25.0.2+10-LTS, mixed mode, sharing) |
| JMH                       | 1.36                                                                                                                                                                                            |
| Kafka                     | 4.3.1 client; `apache/kafka:4.3.0` broker via Testcontainers                                                                                                                                    |
| Atleon                    | 0.47.1                                                                                                                                                                                          |
| KPipe source              | `f823a526b0784aef7f6192e82a3b6a6f0305e7b7` plus captured working-tree changes                                                                                                                   |
| KPipe backpressure fix    | `c4a660d715f7b5a46b3de14e3537404a0a3f0de7` (#357)                                                                                                                                               |
| Docker                    | 29.8.2; 16 CPUs; approximately 15.7 GiB available to Docker                                                                                                                                     |
| Frozen executable SHA-256 | `6b9438f2e8d442305f558602be9161212383edc9b0cae3d2aca0e0c378c70f2c`                                                                                                                              |

The machine was not independently verified to be quiesced. Docker and consumer JVMs share host CPU resources. Both
phases use the identical frozen JAR. Full environment details, source-diff checksums, actual commands, case order, and
timestamps are in the [capture metadata](2026-10-15-m3-max-kpipe-atleon-throughput.metadata.json).

## JMH settings and scope

| Setting                            | Value                                                 |
| ---------------------------------- | ----------------------------------------------------- |
| Forks / benchmark threads          | 5 / 1                                                 |
| Warmup                             | 3 iterations × 2 seconds                              |
| Measurement                        | 5 iterations × 3 seconds                              |
| Profiling                          | `gc`; forced GC enabled                               |
| Cases / measured iteration entries | 30 / 750                                              |
| Records per invocation             | 25,000; eight partitions; 1,000 distinct keys         |
| Metric                             | Records/second via `@OperationsPerInvocation(25_000)` |
| Workloads                          | `workMicros=0,100,1000,10000,100000`                  |

The delays use `LockSupport.parkNanos`: they simulate blocking, rather than CPU work or actual network I/O. Requested
iteration durations can overrun while a complete invocation drains, especially at 100 ms. Each invocation creates a
fresh consumer/group against a pre-seeded topic. Setup and teardown are outside the benchmark timer, but startup after
`start()`, fetching, dispatch, and processing completion are measured. These are finite-batch completion-throughput
measurements, rather than a long-lived steady-state consumer test. No latency-mode benchmarks or
`PerRecordLatencyHarness` were run.

## Implementation and completion semantics

- **KPipe parallel:** virtual thread per record, default in-flight watermarks 10,000/7,000. The included fix lets
  completions unpark a backpressure hold at the low watermark, followed by a zero-timeout poll, avoiding idle time
  inside a paused blocking poll.
- **KPipe per key:** serial queues per byte-content key, concurrent virtual-thread workers across keys; the same
  backpressure fix applies.
- **Atleon parallel:** low-level `KafkaReceiver.receiveManual`, adapted through
  `AloFlux.wrapper(KafkaReceiverRecord::toAloConsumerRecord)`, then Alo `flatMap` with `Integer.MAX_VALUE` concurrency.
- **Atleon per key:** `groupByWithAutoComplete` using `ByteBuffer.wrap(record.key())`, `Integer.MAX_VALUE` group
  concurrency, and serial consumption inside each group with default `innerPublishOn` prefetch.
- **Both Atleon arms:** `Schedulers.fromExecutorService(Executors.newVirtualThreadPerTaskExecutor())`,
  `maxActiveInFlight=10,000`, and explicit `COMPACT_EAGER`. Completion replenishes capacity independently of ordered
  acknowledgement advancement; contiguous completed acknowledgements can compact. Polling retains receiver defaults;
  partition-specific polling overrides are not used here.
- **Raw:** a platform-thread `KafkaConsumer.poll` loop submits every record to a virtual-thread executor. It has no
  equivalent application in-flight limit or processed-offset ledger, and auto-commit is disabled.
- **Share:** a `KafkaShareConsumer` dispatches each polled batch to virtual threads, then waits at a batch barrier
  before polling again, so the next poll can acknowledge the completed batch. It uses a fresh share group configured to
  read the seeded records from earliest.

The matching numerical cap does not equalize backpressure: KPipe uses hysteresis, while Atleon replenishes receiver
capacity on completion. The completion counters measure processing callbacks, excluding final commits/acknowledgement
cleanup. The raw arm provides fewer durability guarantees; the share arm uses a different acquisition/acknowledgement
protocol and a per-batch barrier. These are configured implementation comparisons, not identical control algorithms.

## Throughput

Records/second; higher is better. Errors are relative JMH score errors (99.9% confidence intervals).

| Arm               | Ordering  | 0 µs             | 100 µs           | 1 ms           | 10 ms          | 100 ms         |
| ----------------- | --------- | ---------------- | ---------------- | -------------- | -------------- | -------------- |
| `kpipe`           | Unordered | 562,769 ± 5.0%   | 449,341 ± 6.8%   | 450,477 ± 4.9% | 313,462 ± 3.5% | 70,037 ± 0.7%  |
| `atleonParallel`  | Unordered | 595,115 ± 4.1%   | 834,657 ± 3.2%   | 921,987 ± 5.3% | 488,793 ± 3.5% | 71,813 ± 1.5%  |
| `raw`             | Unordered | 530,299 ± 4.1%   | 486,744 ± 3.4%   | 457,689 ± 4.4% | 410,641 ± 3.8% | 162,017 ± 1.6% |
| `share`           | Unordered | 4,809 ± 0.1%     | 4,797 ± 0.2%     | 4,766 ± 0.2%   | 4,415 ± 0.4%   | 3,004 ± 1.1%   |
| `kpipeKeyOrdered` | Per key   | 338,763 ± 4.1%   | 554,508 ± 5.5%   | 283,202 ± 2.8% | 41,289 ± 0.6%  | 5,031 ± 0.9%   |
| `atleonKey`       | Per key   | 1,595,842 ± 3.4% | 1,173,329 ± 3.1% | 375,151 ± 1.8% | 45,497 ± 0.7%  | 5,535 ± 0.3%   |

## Matched comparisons

| Comparison                      | Ordering           | 0 µs    | 100 µs  | 1 ms    | 10 ms   | 100 ms |
| ------------------------------- | ------------------ | ------- | ------- | ------- | ------- | ------ |
| `atleonParallel` / `kpipe`      | Parallel           | 1.06×   | 1.86×   | 2.05×   | 1.56×   | 1.03×  |
| `atleonKey` / `kpipeKeyOrdered` | Per key            | 4.71×   | 2.12×   | 1.32×   | 1.10×   | 1.10×  |
| `atleonParallel` / `raw`        | Unordered baseline | 1.12×   | 1.71×   | 2.01×   | 1.19×   | 0.44×  |
| `kpipe` / `raw`                 | Unordered baseline | 1.06×   | 0.92×   | 0.98×   | 0.76×   | 0.43×  |
| `atleonParallel` / `share`      | Share baseline     | 123.75× | 173.99× | 193.44× | 110.70× | 23.90× |
| `kpipe` / `share`               | Share baseline     | 117.02× | 93.67×  | 94.51×  | 70.99×  | 23.31× |

Ratios divide sample means and have no independently estimated confidence intervals. The raw/share rows require the
implementation caveats above. The baseline phase ran after the matched-library phase; ratios spanning phases can also
reflect host drift.

### Observations

Parallel: Atleon/KPipe mean-throughput ratios range from **1.03× to 2.05×** across the five workloads. At 1 ms the ratio
is 2.05×, at 10 ms 1.56×, and at 100 ms 1.03×. These describe this capture; they do not by themselves establish
production performance or explain the mechanism.

Per key: Atleon/KPipe mean-throughput ratios range from **1.10× to 4.71×** across the five workloads. At 1 ms the ratio
is 1.32×, at 10 ms 1.10×, and at 100 ms 1.10×. These describe this capture; they do not by themselves establish
production performance or explain the mechanism.

At 100 ms, the raw consumer reaches **162,017 records/s**, compared with **71,813** for Atleon parallel and **70,037**
for KPipe parallel. Raw has no application in-flight cap and can dispatch more of the 25,000-record batch concurrently.
Its larger admission window and absent processed-offset ledger prevent interpreting this result as a measurement of
framework overhead alone; the specific contribution of each difference was not isolated.

The share consumer measures 4,809, 4,797, and 4,766 records/s at 0 µs, 100 µs, and 1 ms. This nearly flat response is
consistent with a large fixed component in the finite-batch measurement. The capture does not isolate group startup,
fetching, or acknowledgement costs. Its large gaps versus the other arms should not be generalized to a long-lived share
consumer without further measurements.

## Comparison with the previous published report

| Atleon ordering | Delay  | Previous records/s | Current records/s | Change |
| --------------- | ------ | ------------------ | ----------------- | ------ |
| Parallel        | 0 µs   | 613,902            | 595,115           | -3.1%  |
| Parallel        | 100 µs | 866,523            | 834,657           | -3.7%  |
| Parallel        | 1 ms   | 937,623            | 921,987           | -1.7%  |
| Per key         | 0 µs   | 1,544,744          | 1,595,842         | +3.3%  |
| Per key         | 100 µs | 1,120,818          | 1,173,329         | +4.7%  |
| Per key         | 1 ms   | 392,184            | 375,151           | -4.3%  |

Across these six overlapping cases, Atleon's mean percentage throughput change is **-0.8%**. Its unweighted average lead
over KPipe changes from **274%** to **119%**. These arithmetic averages give each case equal weight; they are not ratios
of summed throughput or confidence intervals.

The baseline is the [previous full-suite report](2026-10-12-full-suite.md), including its October 13 STRICT Atleon
refresh. Its KPipe results predate the backpressure fix. The current Atleon configuration uses COMPACT_EAGER. Only 0 µs,
100 µs, and 1 ms overlap; the old report has no 10 ms or 100 ms cases. These are cross-capture comparisons, not
controlled estimates of either the fix or the queue-mode effect.

## Allocation and garbage collection

Profiler-reported allocated bytes per record from the throughput trials. Fixture allocations can be included even when
setup/teardown is excluded from the benchmark timer; these figures do not isolate allocations in the processing path.

These figures were measured with JMH 1.36, whose `gc` profiler drops allocation made by threads that exit during an
iteration. The raw, Atleon and share arms run their consumers on such threads, so their figures here are under-counted,
while KPipe's are close to correct. The [2026-10-16 capture](2026-10-16-ci-parallel-jmh137.md) repeats the measurement
with JMH 1.37.

| Arm               | 0 µs    | 100 µs  | 1 ms    | 10 ms   | 100 ms  |
| ----------------- | ------- | ------- | ------- | ------- | ------- |
| `kpipe`           | 1,092.6 | 1,885.5 | 1,971.4 | 1,944.7 | 1,739.2 |
| `atleonParallel`  | 428.8   | 1,020.0 | 1,001.9 | 1,096.1 | 1,011.8 |
| `raw`             | 55.5    | 447.9   | 447.1   | 445.9   | 445.7   |
| `share`           | 33.0    | 424.8   | 424.8   | 424.7   | 424.8   |
| `kpipeKeyOrdered` | 1,194.0 | 911.6   | 930.5   | 960.0   | 946.2   |
| `atleonKey`       | 694.9   | 618.5   | 741.8   | 704.4   | 1,275.9 |

The following GC columns are the native JMH secondary-metric scores, rather than totals recomputed across forks. A dash
indicates that JMH did not emit a finite score for that metric.

| Arm               | Delay  | Allocation rate (MB/s) | GC count score | GC time score (ms) |
| ----------------- | ------ | ---------------------- | -------------- | ------------------ |
| `kpipe`           | 0 µs   | 45.9                   | 78.0           | 75.0               |
| `kpipe`           | 100 µs | 78.3                   | 301.0          | 903.0              |
| `kpipe`           | 1 ms   | 81.9                   | 260.0          | 842.0              |
| `kpipe`           | 10 ms  | 78.1                   | 295.0          | 978.0              |
| `kpipe`           | 100 ms | 53.1                   | 146.0          | 406.0              |
| `atleonParallel`  | 0 µs   | 241.4                  | 500.0          | 644.0              |
| `atleonParallel`  | 100 µs | 799.9                  | 323.0          | 690.0              |
| `atleonParallel`  | 1 ms   | 870.4                  | 323.0          | 748.0              |
| `atleonParallel`  | 10 ms  | 503.1                  | 293.0          | 635.0              |
| `atleonParallel`  | 100 ms | 68.6                   | 262.0          | 822.0              |
| `raw`             | 0 µs   | 2.3                    | 67.0           | 68.0               |
| `raw`             | 100 µs | 18.8                   | 195.0          | 591.0              |
| `raw`             | 1 ms   | 18.8                   | 265.0          | 793.0              |
| `raw`             | 10 ms  | 18.7                   | 241.0          | 813.0              |
| `raw`             | 100 ms | 18.5                   | 260.0          | 874.0              |
| `share`           | 0 µs   | 0.1                    | 0.0            | —                  |
| `share`           | 100 µs | 1.9                    | 7.0            | 6.0                |
| `share`           | 1 ms   | 1.9                    | 5.0            | 4.0                |
| `share`           | 10 ms  | 1.8                    | 6.0            | 2.0                |
| `share`           | 100 ms | 1.2                    | 6.0            | 4.0                |
| `kpipeKeyOrdered` | 0 µs   | 47.9                   | 78.0           | 65.0               |
| `kpipeKeyOrdered` | 100 µs | 38.4                   | 74.0           | 131.0              |
| `kpipeKeyOrdered` | 1 ms   | 37.6                   | 72.0           | 120.0              |
| `kpipeKeyOrdered` | 10 ms  | 26.0                   | 48.0           | 90.0               |
| `kpipeKeyOrdered` | 100 ms | 4.2                    | 0.0            | —                  |
| `atleonKey`       | 0 µs   | 1,041.4                | 278.0          | 799.0              |
| `atleonKey`       | 100 µs | 682.5                  | 277.0          | 684.0              |
| `atleonKey`       | 1 ms   | 262.0                  | 216.0          | 493.0              |
| `atleonKey`       | 10 ms  | 30.2                   | 186.0          | 503.0              |
| `atleonKey`       | 100 ms | 6.7                    | 0.0            | —                  |

## Variability and limitations

Per-fork means and within-case variation help assess stability beyond the aggregate score. CV is the sample standard
deviation of the five fork means divided by their mean; min/max are individual measured iteration throughput scores.

| Arm               | Delay  | Iteration min | Iteration max | Fork-mean CV |
| ----------------- | ------ | ------------- | ------------- | ------------ |
| `kpipe`           | 0 µs   | 506,258       | 645,005       | 4.1%         |
| `kpipe`           | 100 µs | 336,834       | 523,884       | 3.9%         |
| `kpipe`           | 1 ms   | 402,727       | 504,700       | 2.6%         |
| `kpipe`           | 10 ms  | 275,319       | 336,064       | 1.8%         |
| `kpipe`           | 100 ms | 69,097        | 71,411        | 0.5%         |
| `atleonParallel`  | 0 µs   | 512,805       | 648,578       | 3.0%         |
| `atleonParallel`  | 100 µs | 783,156       | 905,361       | 2.7%         |
| `atleonParallel`  | 1 ms   | 807,466       | 1,034,566     | 4.5%         |
| `atleonParallel`  | 10 ms  | 439,711       | 528,338       | 1.6%         |
| `atleonParallel`  | 100 ms | 68,082        | 73,928        | 0.8%         |
| `raw`             | 0 µs   | 465,911       | 578,032       | 1.0%         |
| `raw`             | 100 µs | 442,734       | 534,583       | 3.1%         |
| `raw`             | 1 ms   | 413,133       | 515,435       | 5.0%         |
| `raw`             | 10 ms  | 369,577       | 446,979       | 2.2%         |
| `raw`             | 100 ms | 154,839       | 168,189       | 0.8%         |
| `share`           | 0 µs   | 4,787         | 4,822         | 0.0%         |
| `share`           | 100 µs | 4,774         | 4,812         | 0.1%         |
| `share`           | 1 ms   | 4,742         | 4,789         | 0.1%         |
| `share`           | 10 ms  | 4,367         | 4,462         | 0.3%         |
| `share`           | 100 ms | 2,948         | 3,095         | 1.7%         |
| `kpipeKeyOrdered` | 0 µs   | 303,024       | 376,237       | 2.2%         |
| `kpipeKeyOrdered` | 100 µs | 479,627       | 611,651       | 4.4%         |
| `kpipeKeyOrdered` | 1 ms   | 260,701       | 305,859       | 0.4%         |
| `kpipeKeyOrdered` | 10 ms  | 40,610        | 41,963        | 0.2%         |
| `kpipeKeyOrdered` | 100 ms | 4,884         | 5,095         | 1.3%         |
| `atleonKey`       | 0 µs   | 1,410,041     | 1,692,130     | 1.0%         |
| `atleonKey`       | 100 µs | 1,018,979     | 1,228,415     | 1.8%         |
| `atleonKey`       | 1 ms   | 360,197       | 394,962       | 1.0%         |
| `atleonKey`       | 10 ms  | 44,724        | 46,274        | 0.4%         |
| `atleonKey`       | 100 ms | 5,502         | 5,588         | 0.2%         |

0 cases have relative primary score error above 25%. All measured iterations are retained. Five forks and confidence
intervals do not capture every source of cross-run drift, workload realism, or production variability.

Atleon teardown cancels its subscription and immediately disposes its scheduler. The captured logs contain **1290
mentions of `RejectedExecutionException`** across all cases; these are log-text occurrence counts, not independent
failure counts. Every reported JMH case completed with its full five-fork measurement set. Cleanup and dropped-error
logging may still add noise.

Cases ran serially. Matched library order alternates by workload; raw/share baselines run in a later phase with their
order alternating too. Broker state, background activity, thermal effects, timer scheduling, and fixed phase order can
affect the results. No Linux `perf`/CPI counters were collected. This capture does not establish individual-record tail
latency, final-commit latency, recovery behavior, retry/DLQ behavior, or an apples-to-apples explanation of framework
overhead.

## Reproduction and raw output

Each expanded arm/workload case ran separately with:

```bash
java -jar <frozen-benchmark.jar> \
  "^io.github.eschizoid.kpipe.benchmarks.ParallelProcessingBenchmark.<arm>$" \
  -bm thrpt -p workMicros=<0|100|1000|10000|100000> \
  -wi 3 -w 2s -i 5 -r 3s -f 5 -t 1 \
  -prof gc -gc true -foe true \
  -jvmArgsAppend "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn" \
  -rf json -rff <case-output.json>
```

[Raw JMH JSON](2026-10-15-m3-max-kpipe-atleon-throughput.json) retains 30 native result objects and all 750 measured
iteration entries, including GC metrics. [Capture metadata](2026-10-15-m3-max-kpipe-atleon-throughput.metadata.json)
records both phases, provenance, and settings. The frozen executable, source snapshots, runner scripts, and per-case
logs are retained locally under `/private/tmp/kpipe-atleon-throughput-2026-10-15/`. Earlier published reports and
preliminary captures were not modified.
