package org.kpipe;

import com.google.protobuf.Message;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HexFormat;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.kpipe.format.avro.AvroFormat;
import org.kpipe.format.json.JsonFormat;
import org.kpipe.format.protobuf.ProtobufFormat;
import org.kpipe.registry.MessageFormat;
import org.kpipe.sink.MessageSink;

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
/// This facade is purely additive — it delegates to the existing
/// [org.kpipe.registry.MessageProcessorRegistry] / [org.kpipe.consumer.KPipeConsumer.Builder] /
/// [org.kpipe.consumer.KPipeRunner.Builder] stack and does not replace any public API.
public final class KPipe {

  private static final Logger LOGGER = System.getLogger(KPipe.class.getName());

  private KPipe() {}

  /// Creates a JSON-typed stream that consumes from the given topic. Messages are deserialized
  /// to `Map<String, Object>`.
  ///
  /// @param topic the Kafka topic to consume
  /// @param kafkaProps the Kafka consumer properties
  /// @return a fluent [Stream] configured for JSON
  public static Stream<Map<String, Object>> json(final String topic, final Properties kafkaProps) {
    return new DefaultStream<>(topic, kafkaProps, JsonFormat.INSTANCE, JsonFormat::consoleSink);
  }

  /// Creates a JSON-typed stream that consumes from multiple homogeneous topics through a single
  /// shared pipeline. All topics must produce the same payload shape.
  ///
  /// @param topics the Kafka topics to consume (must be non-empty)
  /// @param kafkaProps the Kafka consumer properties
  /// @return a fluent [Stream] configured for JSON
  public static Stream<Map<String, Object>> json(final Collection<String> topics, final Properties kafkaProps) {
    return new DefaultStream<>(topics, kafkaProps, JsonFormat.INSTANCE, JsonFormat::consoleSink);
  }

  /// Creates an Avro-typed stream that consumes from the given topic. Messages are deserialized
  /// to `GenericRecord` using the format's configured default schema.
  ///
  /// `toConsole()` will throw [IllegalStateException] if no default schema has been registered
  /// under key `"1"` on [AvroFormat#INSTANCE], since the console sink needs the schema to
  /// re-encode payloads as JSON.
  ///
  /// @param topic the Kafka topic to consume
  /// @param kafkaProps the Kafka consumer properties
  /// @return a fluent [Stream] configured for Avro
  public static Stream<GenericRecord> avro(final String topic, final Properties kafkaProps) {
    return new DefaultStream<>(topic, kafkaProps, AvroFormat.INSTANCE, AvroFormat::defaultConsoleSink);
  }

  /// Multi-topic Avro variant. See [#avro(String, Properties)] for the schema requirement.
  ///
  /// @param topics the Kafka topics to consume (must be non-empty)
  /// @param kafkaProps the Kafka consumer properties
  /// @return a fluent [Stream] configured for Avro
  public static Stream<GenericRecord> avro(final Collection<String> topics, final Properties kafkaProps) {
    return new DefaultStream<>(topics, kafkaProps, AvroFormat.INSTANCE, AvroFormat::defaultConsoleSink);
  }

  /// Creates a Protobuf-typed stream that consumes from the given topic. Messages are
  /// deserialized to `com.google.protobuf.Message`.
  ///
  /// @param topic the Kafka topic to consume
  /// @param kafkaProps the Kafka consumer properties
  /// @return a fluent [Stream] configured for Protobuf
  public static Stream<Message> protobuf(final String topic, final Properties kafkaProps) {
    return new DefaultStream<>(topic, kafkaProps, ProtobufFormat.INSTANCE, ProtobufFormat::consoleSink);
  }

  /// Multi-topic Protobuf variant.
  ///
  /// @param topics the Kafka topics to consume (must be non-empty)
  /// @param kafkaProps the Kafka consumer properties
  /// @return a fluent [Stream] configured for Protobuf
  public static Stream<Message> protobuf(final Collection<String> topics, final Properties kafkaProps) {
    return new DefaultStream<>(topics, kafkaProps, ProtobufFormat.INSTANCE, ProtobufFormat::consoleSink);
  }

  /// Creates a raw `byte[]` stream — identity passthrough, no SerDe.
  ///
  /// `toConsole()` returns a sink that logs a UTF-8 preview when the bytes look like text and a
  /// hex preview otherwise.
  ///
  /// @param topic the Kafka topic to consume
  /// @param kafkaProps the Kafka consumer properties
  /// @return a fluent [Stream] for raw bytes
  public static Stream<byte[]> bytes(final String topic, final Properties kafkaProps) {
    return new DefaultStream<>(topic, kafkaProps, MessageFormat.bytes(), KPipe::bytesConsoleSink);
  }

  /// Multi-topic raw `byte[]` variant.
  ///
  /// @param topics the Kafka topics to consume (must be non-empty)
  /// @param kafkaProps the Kafka consumer properties
  /// @return a fluent [Stream] for raw bytes
  public static Stream<byte[]> bytes(final Collection<String> topics, final Properties kafkaProps) {
    return new DefaultStream<>(topics, kafkaProps, MessageFormat.bytes(), KPipe::bytesConsoleSink);
  }

  /// Creates a stream backed by a user-supplied [MessageFormat]. The default `toConsole()` for
  /// custom streams logs values via `String.valueOf(value)` — pass `toCustom(...)` for richer
  /// formatting.
  ///
  /// @param topic the Kafka topic to consume
  /// @param kafkaProps the Kafka consumer properties
  /// @param format the message format
  /// @param <T> the deserialized message type
  /// @return a fluent [Stream] for the supplied format
  public static <T> Stream<T> custom(final String topic, final Properties kafkaProps, final MessageFormat<T> format) {
    return new DefaultStream<>(topic, kafkaProps, format, () -> value -> LOGGER.log(Level.INFO, "{0}", value));
  }

  /// Multi-topic custom-format variant.
  ///
  /// @param topics the Kafka topics to consume (must be non-empty)
  /// @param kafkaProps the Kafka consumer properties
  /// @param format the message format
  /// @param <T> the deserialized message type
  /// @return a fluent [Stream] for the supplied format
  public static <T> Stream<T> custom(
    final Collection<String> topics,
    final Properties kafkaProps,
    final MessageFormat<T> format
  ) {
    return new DefaultStream<>(topics, kafkaProps, format, () -> value -> LOGGER.log(Level.INFO, "{0}", value));
  }

  /// Creates a heterogeneous multi-topic builder. Each route registers a per-topic pipeline,
  /// so different topics can carry different payload shapes through one consumer-group / one
  /// offset manager.
  ///
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

  /// Returns a [MessageSink] that prints `byte[]` values as either UTF-8 (when the payload looks
  /// like text) or as a hex preview. This is the default sink used by [#bytes] for `toConsole`.
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
      final var rendered = renderBytes(value);
      LOGGER.log(Level.INFO, "{0}", rendered);
    };
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
