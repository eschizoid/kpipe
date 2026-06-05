package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.consumer.config.AppConfig;
import io.github.eschizoid.kpipe.consumer.config.KafkaConsumerConfig;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/// JSON consumer that demonstrates `.withCircuitBreaker(...)`. The terminal sink simulates a
/// downstream that fails the first ten records (50%+ failure rate over a window of 5 ⇒ breaker
/// trips), then recovers — driving the CLOSED → OPEN → HALF_OPEN → CLOSED cycle in one run.
///
/// Watch the logs for `circuit breaker tripped`, `half-open probe`, and `breaker closed`. The
/// JSON metrics endpoint (via `handle.metrics()`) exposes `circuitBreakerTrips` and
/// `circuitBreakerTimeOpenMs`.
public final class App {

  private static final Logger LOGGER = System.getLogger(App.class.getName());

  private App() {}

  static void main() {
    final var config = AppConfig.fromEnv();
    final var props = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());

    final var processed = new AtomicInteger(0);
    final MessageSink<Map<String, Object>> flakySink = msg -> {
      final var n = processed.incrementAndGet();
      if (n <= 10) throw new RuntimeException("simulated downstream failure #" + n);
      LOGGER.log(Level.INFO, "delivered record #{0}: {1}", n, msg);
    };

    try (
      final var handle = KPipe.json(config.topic(), props)
        .withProcessingMode(ProcessingMode.SEQUENTIAL)
        .withCircuitBreaker(0.5, 5, Duration.ofMillis(500))
        .toCustom(flakySink)
        .start()
    ) {
      LOGGER.log(Level.INFO, "Circuit-breaker consumer started for topic {0}", config.topic());
      handle.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in circuit-breaker consumer", e);
      System.exit(1);
    }
  }
}
