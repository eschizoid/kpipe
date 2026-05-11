package org.kpipe.producer.tracing;

/// Zero-cost default [Tracer.SpanScope] returned by [NoopTracer]. Package-private — callers reach
/// it through [Tracer.SpanScope#noop()].
final class NoopSpanScope implements Tracer.SpanScope {

  static final NoopSpanScope INSTANCE = new NoopSpanScope();

  private NoopSpanScope() {}

  @Override
  public void recordException(final Throwable t) {
    // no-op
  }

  @Override
  public void close() {
    // no-op
  }
}
