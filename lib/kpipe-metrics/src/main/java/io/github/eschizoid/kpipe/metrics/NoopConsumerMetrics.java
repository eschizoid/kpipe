package io.github.eschizoid.kpipe.metrics;

/// No-op implementation of [ConsumerMetrics]. Singleton; zero allocation per call.
final class NoopConsumerMetrics implements ConsumerMetrics {

  static final NoopConsumerMetrics INSTANCE = new NoopConsumerMetrics();

  private NoopConsumerMetrics() {}

  @Override
  public void recordMessageReceived() {}

  @Override
  public void recordMessageProcessed() {}

  @Override
  public void recordProcessingError() {}

  @Override
  public void recordProcessingDuration(final long millis) {}

  @Override
  public void recordBackpressurePause() {}

  @Override
  public void recordBackpressureTime(final long millis) {}
}
