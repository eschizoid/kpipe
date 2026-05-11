package org.kpipe.tracing.otel;

import io.opentelemetry.context.propagation.TextMapSetter;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Headers;

/// [TextMapSetter] adapter that writes UTF-8 string values into Kafka [Headers]. Used by
/// [OtelTracer#injectContextInto] to add `traceparent` / `tracestate` to outbound DLQ records and
/// sink-produced records.
///
/// Replaces existing headers with the same key so re-injection on a retried record yields a single
/// up-to-date `traceparent`.
final class HeadersTextMapSetter implements TextMapSetter<Headers> {

  static final HeadersTextMapSetter INSTANCE = new HeadersTextMapSetter();

  private HeadersTextMapSetter() {}

  @Override
  public void set(final Headers carrier, final String key, final String value) {
    if (carrier == null) return;
    carrier.remove(key);
    carrier.add(key, value.getBytes(StandardCharsets.UTF_8));
  }
}
