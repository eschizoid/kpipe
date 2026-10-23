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

  /// Timeout of the Kafka client's own close at shutdown, which leaves the group and closes its
  /// connections. Kafka's default is 30 seconds, and a broker that cannot be reached makes the
  /// close wait that long, past any join budget.
  static final Duration KAFKA_CLIENT_CLOSE = Duration.ofMillis(5000);

  /// Timeout of the producer's close at shutdown, for a consumer with a producer for its
  /// dead-letter queue or Kafka sinks. A producer closed without one waits up to its
  /// `delivery.timeout.ms`, two minutes by default, for sends a broker cannot acknowledge.
  static final Duration PRODUCER_CLOSE = Duration.ofMillis(5000);

  /// Default max time `close()` waits for the consumer thread to join before interrupting it,
  /// for a consumer in `mode`. It covers each wait that mode's teardown can run, through the
  /// final commit and the Kafka clients' closes, so a stuck record or an unreachable broker
  /// does not get the thread interrupted mid-teardown, and no wait the mode never runs.
  /// `close()` first waits up to [#WAIT_FOR_MESSAGES] on its own thread, so its worst case
  /// is that plus this budget.
  ///
  /// SEQUENTIAL runs each record on the consumer thread, so by the time its loop exits nothing is
  /// in flight and no dispatcher waits; a record hung on that thread keeps the loop from exiting
  /// at all, and no budget changes that.
  ///
  /// @param mode        the consumer's processing mode
  /// @param hasProducer whether the consumer closes a producer for a dead-letter queue or sinks
  /// @return the default join budget for that consumer
  static Duration threadTermination(final ProcessingMode mode, final boolean hasProducer) {
    final var clientCloses = hasProducer ? KAFKA_CLIENT_CLOSE.plus(PRODUCER_CLOSE) : KAFKA_CLIENT_CLOSE;
    final var afterDispatch = COMMIT_SCHEDULER_STOP.plus(FINAL_COMMIT).plus(clientCloses);
    return switch (mode) {
      case SEQUENTIAL -> afterDispatch;
      case PARALLEL -> WAIT_FOR_MESSAGES.plus(EXECUTOR_TERMINATION).plus(afterDispatch);
      case KEY_ORDERED -> WAIT_FOR_MESSAGES.plus(KEY_ORDERED_CLOSE_DRAIN).plus(afterDispatch);
    };
  }

  private ConsumerDefaults() {}
}
