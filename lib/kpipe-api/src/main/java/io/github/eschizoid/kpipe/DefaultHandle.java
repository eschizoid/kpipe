package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/// Package-private [Handle] impl wrapping a started [KPipeConsumer]. Since the 1.13 fold of
/// `KPipeRunner` into `KPipeConsumer`, every lifecycle concern lives on the consumer directly —
/// no intermediate runner.
record DefaultHandle(KPipeConsumer consumer) implements Handle {
  DefaultHandle(final KPipeConsumer consumer) {
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

  @Override
  public List<Map.Entry<byte[], Integer>> topKeyQueueDepths(final int n) {
    return consumer.topKeyQueueDepths(n);
  }

  /// Starts `consumer` and returns it wrapped in a `DefaultHandle`. If `start()` throws, closes
  /// the consumer (suppressing any secondary exception onto the original) so the
  /// `AutoCloseable` contract still holds when no `Handle` is returned to the caller.
  ///
  /// Shared by [DefaultSink#start()], [DefaultBatchSink#start()], and
  /// [MultiBuilder#start()] — the only three facade-level start sites — so the
  /// start-and-wrap dance lives in one place.
  static Handle startAndWrap(final KPipeConsumer consumer) {
    Objects.requireNonNull(consumer, "consumer cannot be null");
    try {
      consumer.start();
    } catch (final RuntimeException e) {
      try {
        consumer.close();
      } catch (final Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
    return new DefaultHandle(consumer);
  }
}
