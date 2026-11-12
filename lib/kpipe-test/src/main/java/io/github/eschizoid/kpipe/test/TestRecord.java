package io.github.eschizoid.kpipe.test;

import io.github.eschizoid.kpipe.record.RecordContext;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

/// A record to send through a [TestStream] or seed into a [CrashRestartHarness]: a value with an
/// optional key, headers and timestamp. Each `with` method returns a new record.
///
/// ```java
/// driver.send(TestRecord.of(order).withKey("order-1").withHeader("tenant", "acme"));
/// ```
///
/// [#context()] presents the record as the consumer would, for unit-testing an operator that
/// reads [RecordContext#current()]:
///
/// ```java
/// final var record = TestRecord.of(order).withKey("order-1");
/// RecordContext.callWith(record.context(), () -> enrich.apply(record.value()));
/// ```
///
/// @param <T> the record value type
public final class TestRecord<T> {

  /// The topic [#context()] reports.
  public static final String DEFAULT_TOPIC = "kpipe-test-topic";

  private final T value;
  private final byte[] key;
  private final List<Map.Entry<String, byte[]>> headers;
  private final long timestamp;

  private TestRecord(
    final T value,
    final byte[] key,
    final List<Map.Entry<String, byte[]>> headers,
    final long timestamp
  ) {
    this.value = value;
    this.key = key;
    this.headers = headers;
    this.timestamp = timestamp;
  }

  /// A record with `value`, no key, no headers and no timestamp, so its context reports a
  /// timestamp of -1. A record from a broker always has one; set it with [#withTimestamp] when an
  /// operator reads it.
  ///
  /// @param value the record value
  /// @param <T>   the record value type
  /// @return the record
  /// @throws NullPointerException if `value` is null; use `TestStream.sendRaw` for a null value
  public static <T> TestRecord<T> of(final T value) {
    return new TestRecord<>(Objects.requireNonNull(value, "value cannot be null"), null, List.of(), -1L);
  }

  /// @param key the key, encoded as UTF-8; null for no key
  /// @return a copy of this record with the key
  public TestRecord<T> withKey(final String key) {
    return withKeyBytes(key == null ? null : key.getBytes(StandardCharsets.UTF_8));
  }

  /// @param key the key bytes; null for no key
  /// @return a copy of this record with the key
  public TestRecord<T> withKeyBytes(final byte[] key) {
    return new TestRecord<>(value, key == null ? null : key.clone(), headers, timestamp);
  }

  /// Adds a header after any already on the record; a record may carry one name more than once.
  ///
  /// @param name  the header name
  /// @param value the header value, encoded as UTF-8; may be null
  /// @return a copy of this record with the header
  /// @throws NullPointerException if `name` is null
  public TestRecord<T> withHeader(final String name, final String value) {
    return withHeaderBytes(name, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
  }

  /// Adds a header after any already on the record; a record may carry one name more than once.
  ///
  /// @param name  the header name
  /// @param value the header value; may be null
  /// @return a copy of this record with the header
  public TestRecord<T> withHeaderBytes(final String name, final byte[] value) {
    Objects.requireNonNull(name, "name cannot be null");
    final var next = new ArrayList<>(headers);
    next.add(new SimpleImmutableEntry<>(name, value == null ? null : value.clone()));
    return new TestRecord<>(this.value, key, List.copyOf(next), timestamp);
  }

  /// @param epochMillis the record's create time, in epoch milliseconds
  /// @return a copy of this record with the timestamp
  public TestRecord<T> withTimestamp(final long epochMillis) {
    return new TestRecord<>(value, key, headers, epochMillis);
  }

  /// @return the record value
  public T value() {
    return value;
  }

  /// The record as the consumer would present it, read from [#DEFAULT_TOPIC], partition 0,
  /// offset 0. A [TestStream] reports its own topic and gives each sent record the next offset; a
  /// [CrashRestartHarness] reports its own topic and uses the record's seed index as the offset.
  ///
  /// @return the record's context
  public RecordContext context() {
    return new ConsumedRecordContext(toConsumerRecord(DEFAULT_TOPIC, 0, 0L, new byte[0]));
  }

  /// Builds the consumed record, with `serializedValue` as its value.
  ConsumerRecord<byte[], byte[]> toConsumerRecord(
    final String topic,
    final int partition,
    final long offset,
    final byte[] serializedValue
  ) {
    final var recordHeaders = new RecordHeaders();
    for (final var header : headers) recordHeaders.add(header.getKey(), copy(header.getValue()));
    return new ConsumerRecord<>(
      topic,
      partition,
      offset,
      timestamp,
      timestamp < 0 ? TimestampType.NO_TIMESTAMP_TYPE : TimestampType.CREATE_TIME,
      ConsumerRecord.NULL_SIZE,
      ConsumerRecord.NULL_SIZE,
      copy(key),
      serializedValue,
      recordHeaders,
      Optional.empty()
    );
  }

  /// Each consumed record gets its own arrays, so an operator that changes one in place cannot
  /// change this record or another one built from it.
  private static byte[] copy(final byte[] bytes) {
    return bytes == null ? null : bytes.clone();
  }
}
