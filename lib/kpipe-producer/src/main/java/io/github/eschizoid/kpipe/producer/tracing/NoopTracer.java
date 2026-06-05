package io.github.eschizoid.kpipe.producer.tracing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

/// Zero-cost default [Tracer] used when tracing is not configured. Package-private — callers reach
/// it through [Tracer#noop()].
final class NoopTracer implements Tracer {

  static final NoopTracer INSTANCE = new NoopTracer();

  private NoopTracer() {}

  @Override
  public SpanScope startConsumerSpan(final ConsumerRecord<?, byte[]> record) {
    return SpanScope.noop();
  }

  @Override
  public void injectContextInto(final Headers headers) {
    // no-op
  }
}
