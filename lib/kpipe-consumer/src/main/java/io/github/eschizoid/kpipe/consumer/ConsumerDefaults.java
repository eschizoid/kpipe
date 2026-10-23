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

  /// Max time the key-ordered dispatcher waits for its per-key queues to drain at shutdown.
  static final Duration KEY_ORDERED_CLOSE_DRAIN = Duration.ofMillis(5000);

  /// Default max time `close()` waits for the consumer thread to join before interrupting it, for
  /// a consumer in `mode`. It covers each wait that mode's teardown can run before the final commit
  /// completes, so a stuck record does not get the thread interrupted mid-commit, and no wait the
  /// mode never runs. `close()` first waits up to [#WAIT_FOR_MESSAGES] on its own thread, so its
  /// worst case is that plus this budget.
  ///
  /// SEQUENTIAL runs each record on the consumer thread, so by the time its loop exits nothing is
  /// in flight and no dispatcher waits; a record hung on that thread keeps the loop from exiting
  /// at all, and no budget changes that.
  ///
  /// @param mode the consumer's processing mode
  /// @return the default join budget for that mode
  static Duration threadTermination(final ProcessingMode mode) {
    final var afterDispatch = COMMIT_SCHEDULER_STOP.plus(FINAL_COMMIT);
    return switch (mode) {
      case SEQUENTIAL -> afterDispatch;
      case PARALLEL -> WAIT_FOR_MESSAGES.plus(EXECUTOR_TERMINATION).plus(afterDispatch);
      case KEY_ORDERED -> WAIT_FOR_MESSAGES.plus(KEY_ORDERED_CLOSE_DRAIN).plus(afterDispatch);
    };
  }

  private ConsumerDefaults() {}
}
