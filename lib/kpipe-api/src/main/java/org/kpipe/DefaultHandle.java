package org.kpipe;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import org.kpipe.consumer.KPipeConsumer;

/// Package-private [Handle] impl wrapping a started [KPipeConsumer]. Since the 1.13 fold of
/// `KPipeRunner` into `KPipeConsumer`, every lifecycle concern lives on the consumer directly —
/// no intermediate runner.
record DefaultHandle(KPipeConsumer<byte[]> consumer) implements Handle {
  DefaultHandle(final KPipeConsumer<byte[]> consumer) {
    this.consumer = Objects.requireNonNull(consumer, "consumer cannot be null");
  }

  @Override
  public boolean isHealthy() {
    return consumer.isRunning();
  }

  @Override
  public Map<String, Long> metrics() {
    return consumer.getMetrics();
  }

  @Override
  public boolean awaitShutdown(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout cannot be null");
    return consumer.awaitShutdown(timeout);
  }

  @Override
  public void awaitShutdown() throws InterruptedException {
    consumer.awaitShutdown();
  }

  @Override
  public boolean shutdownGracefully(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout cannot be null");
    return consumer.shutdownGracefully(timeout);
  }
}
