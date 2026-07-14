package io.github.eschizoid.kpipe;

import com.google.protobuf.Message;
import io.github.eschizoid.kpipe.format.avro.AvroFormat;
import io.github.eschizoid.kpipe.format.json.JsonFormat;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.SchemaResolver;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HexFormat;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;

/// Top-level fluent facade for KPipe. Provides static factories for the supported message
/// formats, each returning a [Stream] configured to consume the given Kafka topic.
///
/// The 5-line "hello world" KPipe consumer:
///
/// ```java
/// KPipe.json("events", kafkaProps)
///     .pipe(msg -> { msg.put("ts", System.currentTimeMillis()); return msg; })
///     .pipe(Operators.removeFields("password"))
///     .toConsole()
///     .start();
/// ```
///
/// Each format exposes two overloads: one taking a single `String topic` and one taking a
/// `Collection<String> topics` for homogeneous multi-topic consumption (single shared pipeline).
/// For heterogeneous routes (per-topic typing) use [#multi].
public final class KPipe {

  private static final Logger LOGGER = System.getLogger(KPipe.class.getName());

  private KPipe() {}

  /// JSON-typed stream consuming from `topic`. Messages deserialize to `Map<String, Object>`.
  ///
  /// @param topic the Kafka topic to consume
  /// @param kafkaProps the Kafka consumer properties
  /// @return a fluent [Stream] configured for JSON
  public static Stream<Map<String, Object>> json(final String topic, final Properties kafkaProps) {
    return new DefaultStream<>(topic, kafkaProps, JsonFormat.INSTANCE, JsonFormat::consoleSink);
  }

  /// JSON-typed stream consuming from multiple homogeneous topics through a single shared pipeline.
  ///
  /// @param topics the Kafka topics to consume (must be non-empty)
  /// @param kafkaProps the Kafka consumer properties
  /// @return a fluent [Stream] configured for JSON
  public static Stream<Map<String, Object>> json(final Collection<String> topics, final Properties kafkaProps) {
    return new DefaultStream<>(topics, kafkaProps, JsonFormat.INSTANCE, JsonFormat::consoleSink);
  }

  /// Avro-typed stream consuming from `topic` using `format` for SerDe. The schema bound to
  /// `format` is used for both deserialization and the default `toConsole()` sink. Construct
  /// the format explicitly: `new AvroFormat(schema)` or `AvroFormat.of(schemaJson)`.
  ///
  /// @param topic the Kafka topic to consume
  /// @param kafkaProps the Kafka consumer properties
  /// @param format the Avro codec (must be non-null)
  /// @return a fluent [Stream] configured for Avro
  public static Stream<GenericRecord> avro(final String topic, final Properties kafkaProps, final AvroFormat format) {
    return new DefaultStream<>(topic, kafkaProps, format, format::consoleSink);
  }

  /// Avro-typed stream consuming from multiple homogeneous topics. See
  /// [#avro(String, Properties, AvroFormat)].
  ///
  /// @param topics the Kafka topics to consume (must be non-empty)
  /// @param kafkaProps the Kafka consumer properties
  /// @param format the Avro codec (must be non-null)
  /// @return a fluent [Stream] configured for Avro
  public static Stream<GenericRecord> avro(
    final Collection<String> topics,
    final Properties kafkaProps,
    final AvroFormat format
  ) {
    return new DefaultStream<>(topics, kafkaProps, format, format::consoleSink);
  }

  /// Avro-typed stream wired for per-record Confluent Schema Registry lookup. The wire envelope
  /// is consumed by the format itself; do NOT pair this overload with `.skipBytes(5)`.
  ///
  /// Equivalent to `avro(topic, kafkaProps, AvroFormat.withRegistry(resolver))`. Provided so the
  /// "Confluent Avro topic in one line" pitch is genuinely one line. Wrap the underlying SR
  /// resolver with `CachedSchemaResolver` for the cache; that bookkeeping is your responsibility
  /// since the resolver typically outlives a single stream.
  ///
  /// @param topic the Kafka topic to consume
  /// @param kafkaProps the Kafka consumer properties
  /// @param resolver the schema resolver (must be non-null; typically a `CachedSchemaResolver`)
  /// @return a fluent [Stream] configured for Avro with per-record schema lookup
  public static Stream<GenericRecord> avro(
    final String topic,
    final Properties kafkaProps,
    final SchemaResolver resolver
  ) {
    final var format = AvroFormat.withRegistry(resolver);
    return new DefaultStream<>(topic, kafkaProps, format, KPipe::registryModeConsoleSinkUnsupported);
  }

  private static MessageSink<GenericRecord> registryModeConsoleSinkUnsupported() {
    throw new IllegalStateException(
      "toConsole() requires a fixed schema; an AvroFormat in Schema-Registry mode has none. " +
        "Use .toCustom(...) with your own sink, or construct the stream via " +
        "KPipe.avro(topic, props, new AvroFormat(schema)) to keep the console-sink path."
    );
  }

  /// Protobuf-typed stream consuming from `topic` using `format` for SerDe. Construct the format
  /// explicitly: `new ProtobufFormat(descriptor)`.
  ///
  /// @param topic the Kafka topic to consume
  /// @param kafkaProps the Kafka consumer properties
  /// @param format the Protobuf codec (must be non-null)
  /// @return a fluent [Stream] configured for Protobuf
  public static Stream<Message> protobuf(final String topic, final Properties kafkaProps, final ProtobufFormat format) {
    return new DefaultStream<>(topic, kafkaProps, format, format::consoleSink);
  }

  /// Protobuf-typed stream consuming from multiple homogeneous topics.
  ///
  /// @param topics the Kafka topics to consume (must be non-empty)
  /// @param kafkaProps the Kafka consumer properties
  /// @param format the Protobuf codec (must be non-null)
  /// @return a fluent [Stream] configured for Protobuf
  public static Stream<Message> protobuf(
    final Collection<String> topics,
    final Properties kafkaProps,
    final ProtobufFormat format
  ) {
    return new DefaultStream<>(topics, kafkaProps, format, format::consoleSink);
  }

  /// Protobuf-typed stream wired for per-record Confluent Schema Registry lookup — the mirror of
  /// [#avro(String, Properties, SchemaResolver)]. The 6-byte wire envelope (magic + schema id +
  /// message-index) is consumed by the format itself; do NOT pair this with `.skipBytes(6)`.
  ///
  /// Requires **`kpipe-format-protobuf-confluent`** on the runtime path — it supplies the
  /// `.proto`-text compiler discovered via `ServiceLoader` (protobuf-java has no `.proto` parser).
  /// Equivalent to `protobuf(topic, kafkaProps, ProtobufFormat.withRegistry(resolver))`. Wrap the
  /// resolver with `CachedSchemaResolver` for the cache.
  ///
  /// @param topic the Kafka topic to consume
  /// @param kafkaProps the Kafka consumer properties
  /// @param resolver the schema resolver (must be non-null; typically a `CachedSchemaResolver`)
  /// @return a fluent [Stream] configured for Protobuf with per-record schema lookup
  public static Stream<Message> protobuf(
    final String topic,
    final Properties kafkaProps,
    final SchemaResolver resolver
  ) {
    final var format = ProtobufFormat.withRegistry(resolver);
    return new DefaultStream<>(topic, kafkaProps, format, KPipe::registryModeProtobufConsoleSinkUnsupported);
  }

  private static MessageSink<Message> registryModeProtobufConsoleSinkUnsupported() {
    throw new IllegalStateException(
      "toConsole() requires a fixed descriptor; a ProtobufFormat in Schema-Registry mode has none. " +
        "Use .toCustom(...) with your own sink, or construct the stream via " +
        "KPipe.protobuf(topic, props, new ProtobufFormat(descriptor)) to keep the console-sink path."
    );
  }

  /// Raw `byte[]` stream — identity passthrough, no SerDe. `toConsole()` logs a UTF-8 preview
  /// when the payload looks like text, hex preview otherwise.
  ///
  /// @param topic the Kafka topic to consume
  /// @param kafkaProps the Kafka consumer properties
  /// @return a fluent [Stream] for raw bytes
  public static Stream<byte[]> bytes(final String topic, final Properties kafkaProps) {
    return new DefaultStream<>(topic, kafkaProps, MessageFormat.bytes(), KPipe::bytesConsoleSink);
  }

  /// Raw `byte[]` stream consuming from multiple homogeneous topics.
  ///
  /// @param topics the Kafka topics to consume (must be non-empty)
  /// @param kafkaProps the Kafka consumer properties
  /// @return a fluent [Stream] for raw bytes
  public static Stream<byte[]> bytes(final Collection<String> topics, final Properties kafkaProps) {
    return new DefaultStream<>(topics, kafkaProps, MessageFormat.bytes(), KPipe::bytesConsoleSink);
  }

  /// Stream backed by a user-supplied [MessageFormat]. The default `toConsole()` for custom
  /// streams logs values via `String.valueOf(value)` — pass `toCustom(...)` for richer formatting.
  /// Use this entry point when your payload has its own SerDe (e.g. a hand-rolled binary protocol,
  /// MessagePack, Cap'n Proto) that isn't covered by [#json], [#avro], [#protobuf], or [#bytes].
  ///
  /// @param topic the Kafka topic to consume
  /// @param kafkaProps the Kafka consumer properties
  /// @param format the message format (must not be null)
  /// @param <T> the deserialized message type
  /// @return a fluent [Stream] for the supplied format
  public static <T> Stream<T> custom(final String topic, final Properties kafkaProps, final MessageFormat<T> format) {
    return new DefaultStream<>(topic, kafkaProps, format, KPipe::loggingSink);
  }

  /// Custom-format stream consuming from multiple homogeneous topics through a single shared
  /// pipeline. See [#custom(String, Properties, MessageFormat)] for the per-topic shape.
  ///
  /// @param topics the Kafka topics to consume (must be non-empty)
  /// @param kafkaProps the Kafka consumer properties
  /// @param format the message format (must not be null)
  /// @param <T> the deserialized message type
  /// @return a fluent [Stream] for the supplied format
  public static <T> Stream<T> custom(
    final Collection<String> topics,
    final Properties kafkaProps,
    final MessageFormat<T> format
  ) {
    return new DefaultStream<>(topics, kafkaProps, format, KPipe::loggingSink);
  }

  /// Heterogeneous multi-topic builder. Each route registers a per-topic pipeline so different
  /// topics can carry different payload shapes through one consumer-group / one offset manager.
  /// Records arriving for unrouted topics are dropped at WARNING and their offsets are still
  /// committed.
  ///
  /// ```java
  /// KPipe.multi(props)
  ///     .json("events-json", s -> s.pipe(addTimestamp).toCustom(jsonSink))
  ///     .avro("events-avro", s -> s.filter(active).toCustom(avroSink))
  ///     .start();
  /// ```
  ///
  /// @param kafkaProps the Kafka consumer properties
  /// @return a fluent [MultiBuilder]
  public static MultiBuilder multi(final Properties kafkaProps) {
    return new MultiBuilder(kafkaProps);
  }

  /// `MessageSink` that prints `byte[]` values as either UTF-8 (when the payload looks like
  /// text) or as a hex preview. Used by [#bytes] for `toConsole`.
  ///
  /// @return a console sink for `byte[]` payloads
  public static MessageSink<byte[]> bytesConsoleSink() {
    return value -> {
      if (value == null) {
        LOGGER.log(Level.INFO, "null");
        return;
      }
      if (value.length == 0) {
        LOGGER.log(Level.INFO, "empty");
        return;
      }
      LOGGER.log(Level.INFO, "{0}", renderBytes(value));
    };
  }

  /// Default `toConsole()` sink for [#custom(String, Properties, MessageFormat)] streams. Logs
  /// each value via `String.valueOf(value)` at INFO. Users who want richer formatting should
  /// use `toCustom(...)` with their own [MessageSink] instead.
  private static <T> MessageSink<T> loggingSink() {
    return value -> LOGGER.log(Level.INFO, "{0}", value);
  }

  private static String renderBytes(final byte[] value) {
    if (looksLikeText(value)) return new String(value, StandardCharsets.UTF_8);
    final var preview = value.length > 64 ? java.util.Arrays.copyOf(value, 64) : value;
    final var suffix = value.length > 64 ? "...(%d bytes total)".formatted(value.length) : "";
    return "0x%s%s".formatted(HexFormat.of().formatHex(preview), suffix);
  }

  private static boolean looksLikeText(final byte[] value) {
    final var limit = Math.min(value.length, 256);
    for (int i = 0; i < limit; i++) {
      final var b = value[i] & 0xFF;
      if (b == 0) return false;
      // Allow tab (9), LF (10), CR (13), and any printable ASCII or UTF-8 continuation byte.
      if (b < 0x20 && b != 0x09 && b != 0x0A && b != 0x0D) return false;
    }
    return true;
  }
}
