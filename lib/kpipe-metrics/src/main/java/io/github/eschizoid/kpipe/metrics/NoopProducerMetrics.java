package io.github.eschizoid.kpipe.metrics;

/// No-op implementation of [ProducerMetrics]. Singleton; zero allocation per call.
final class NoopProducerMetrics implements ProducerMetrics {

  static final NoopProducerMetrics INSTANCE = new NoopProducerMetrics();

  private NoopProducerMetrics() {}

  @Override
  public void recordMessageSent() {}

  @Override
  public void recordMessageFailed() {}

  @Override
  public void recordDlqSent() {}
}
