package io.github.eschizoid.kpipe.consumer;

import java.time.Duration;

/// Load-bearing default timeouts for the consumer lifecycle, read by [KPipeConsumerBuilder] when the
/// caller doesn't override them via the corresponding `with*` setters.
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

  /// Max time `close()` waits for the consumer thread to join before giving up.
  static final Duration THREAD_TERMINATION = Duration.ofMillis(5000);

  /// Max time `close()` waits for the dispatcher's executor to drain before `shutdownNow()`.
  static final Duration EXECUTOR_TERMINATION = Duration.ofMillis(10000);

  private ConsumerDefaults() {}
}
