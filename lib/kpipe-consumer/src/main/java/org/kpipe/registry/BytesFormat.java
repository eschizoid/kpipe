package org.kpipe.registry;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/// Identity-passthrough [MessageFormat] for `byte[]` payloads.
///
/// Use this when you want the [TypedPipelineBuilder] ergonomics — fluent `add(...)`
/// and `toSink(...)` — but don't need real deserialization. Common cases:
///
/// - Tests that exercise consumer mechanics (backpressure, retry, offsets) without
///   format machinery.
/// - Benchmarks that just observe message throughput.
/// - Pipelines that route raw bytes (e.g. tee to multiple topics) without parsing.
///
/// Example:
/// ```java
/// final var pipeline = registry.pipeline(MessageFormat.bytes())
///     .add(b -> { metrics.inc(); return b; })
///     .toSink(b -> System.out.println(new String(b)))
///     .build();
/// ```
///
/// Schema operations are no-ops — there are no schemas for raw bytes.
public final class BytesFormat implements MessageFormat<byte[]> {

  static final BytesFormat INSTANCE = new BytesFormat();

  private BytesFormat() {}

  @Override
  public Map<String, SchemaInfo> getSchemas() {
    return Collections.emptyMap();
  }

  @Override
  public Optional<SchemaInfo> findSchema(final String key) {
    return Optional.empty();
  }

  @Override
  public void clearSchemas() {
    // no-op: BytesFormat has no schemas
  }

  @Override
  public void addSchema(final String key, final String fullyQualifiedName, final String location) {
    // no-op: BytesFormat has no schemas
  }

  @Override
  public List<SchemaInfo> findSchemas(final Predicate<SchemaInfo> predicate) {
    return List.of();
  }

  @Override
  public byte[] serialize(final byte[] data) {
    return data;
  }

  @Override
  public byte[] deserialize(final byte[] data) {
    return data;
  }
}
