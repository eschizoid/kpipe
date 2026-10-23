package io.github.eschizoid.kpipe.consumer;

import java.time.Duration;

/// Load-bearing default timeouts for the consumer lifecycle, read by [KPipeConsumerBuilder] when
/// the caller doesn't override them via the corresponding `with*` setters.
///
/// These live here, next to the consumer, rather than on the demo `AppConfig` record: the builder's
/// defaults are core behaviour, and coupling them to `AppConfig`'s `fromEnv()` env-parsing
/// scaffolding meant a change to that demo config could ripple into the consumer's shutdown timing.
final class ConsumerDefaults {

  /// Default `Consumer.poll` timeout — bounds each consumer-loop iteration (and therefore the
  /// paused-loop backpressure re-check cadence).
  static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

  /// Max time `waitForInFlightDrain` / graceful shutdown waits for in-flight records to finish.
  static final Duration WAIT_FOR_MESSAGES = Duration.ofMillis(5000);

  /// Max time `close()` waits for the dispatcher's executor to drain before `shutdownNow()`.
  static final Duration EXECUTOR_TERMINATION = Duration.ofMillis(10000);

  /// Timeout of the offset manager's final commit at shutdown.
  static final Duration FINAL_COMMIT = Duration.ofMillis(5000);

  /// Max time the offset manager waits for its periodic-commit scheduler to stop at shutdown. A
  /// periodic commit that fires during teardown waits on a command queue nobody serves anymore, so
  /// this wait can run in full.
  static final Duration COMMIT_SCHEDULER_STOP = Duration.ofMillis(5000);

  /// Max time `close()` waits for the consumer thread to join before interrupting it. It covers
  /// each wait the thread's teardown can run before the final commit completes: the in-flight
  /// drain, the dispatcher's termination wait, the commit scheduler's stop and the final commit. A
  /// shorter budget interrupts the thread while a stuck record holds the dispatcher, and the
  /// interrupted final commit then throws before its result is known. `close()` first waits up to
  /// [#WAIT_FOR_MESSAGES] on its own thread, so its worst case is that plus this budget.
  static final Duration THREAD_TERMINATION = WAIT_FOR_MESSAGES.plus(EXECUTOR_TERMINATION)
    .plus(COMMIT_SCHEDULER_STOP)
    .plus(FINAL_COMMIT);

  private ConsumerDefaults() {}
}
