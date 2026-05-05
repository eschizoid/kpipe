package org.kpipe.registry;

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
public final class BytesFormat implements MessageFormat<byte[]> {

  static final BytesFormat INSTANCE = new BytesFormat();

  private BytesFormat() {}

  @Override
  public byte[] serialize(final byte[] data) {
    return data;
  }

  @Override
  public byte[] deserialize(final byte[] data) {
    return data;
  }
}
