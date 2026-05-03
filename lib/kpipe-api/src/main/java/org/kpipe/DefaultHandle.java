package org.kpipe;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.consumer.KPipeRunner;

/// Package-private [Handle] impl wrapping a started [KPipeRunner].
final class DefaultHandle implements Handle {

  private final KPipeRunner<KPipeConsumer<byte[]>> runner;
  private final KPipeConsumer<byte[]> consumer;

  DefaultHandle(final KPipeRunner<KPipeConsumer<byte[]>> runner, final KPipeConsumer<byte[]> consumer) {
    this.runner = Objects.requireNonNull(runner, "runner cannot be null");
    this.consumer = Objects.requireNonNull(consumer, "consumer cannot be null");
  }

  @Override
  public boolean isHealthy() {
    return runner.isHealthy();
  }

  @Override
  public Map<String, Long> metrics() {
    return consumer.getMetrics();
  }

  @Override
  public boolean awaitShutdown(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout cannot be null");
    return runner.awaitShutdown(timeout.toMillis());
  }

  @Override
  public boolean shutdownGracefully(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout cannot be null");
    return runner.shutdownGracefully(timeout.toMillis());
  }
}
