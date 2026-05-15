package org.kpipe;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.consumer.KPipeRunner;

/// Package-private [Handle] impl wrapping a started [KPipeRunner].
record DefaultHandle(KPipeRunner<KPipeConsumer<byte[]>> runner, KPipeConsumer<byte[]> consumer) implements Handle {
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
  public void awaitShutdown() throws InterruptedException {
    runner.awaitShutdown();
  }

  @Override
  public boolean shutdownGracefully(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout cannot be null");
    return runner.shutdownGracefully(timeout.toMillis());
  }

  /// Starts `consumer` and returns it wrapped in a `DefaultHandle`. If `start()` throws, closes
  /// the consumer (suppressing any secondary exception onto the original) so the
  /// `AutoCloseable` contract still holds when no `Handle` is returned to the caller.
  ///
  /// Shared by [DefaultSink#start()], [DefaultBatchSink#start()], and
  /// [MultiBuilder#start()] — the only three facade-level start sites — so the
  /// start-and-wrap dance lives in one place.
  static Handle startAndWrap(final KPipeConsumer<byte[]> consumer) {
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
