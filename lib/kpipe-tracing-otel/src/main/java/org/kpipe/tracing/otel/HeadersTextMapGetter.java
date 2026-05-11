package org.kpipe.tracing.otel;

import io.opentelemetry.context.propagation.TextMapGetter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/// [TextMapGetter] adapter that reads UTF-8 string values from Kafka [Headers]. Used by
/// [OtelTracer#startConsumerSpan] to extract `traceparent` / `tracestate` from inbound records.
final class HeadersTextMapGetter implements TextMapGetter<Headers> {

  static final HeadersTextMapGetter INSTANCE = new HeadersTextMapGetter();

  private HeadersTextMapGetter() {}

  @Override
  public Iterable<String> keys(final Headers carrier) {
    if (carrier == null) return Collections.emptyList();
    final var keys = new ArrayList<String>();
    for (final Header h : carrier) keys.add(h.key());
    return keys;
  }

  @Override
  public String get(final Headers carrier, final String key) {
    if (carrier == null) return null;
    final var header = carrier.lastHeader(key);
    if (header == null || header.value() == null) return null;
    return new String(header.value(), StandardCharsets.UTF_8);
  }
}
