package org.kpipe.producer.sink;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.function.Function;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kpipe.sink.MessageSink;

/// A [MessageSink] that sends processed messages to a Kafka topic.
///
/// Send failures are reported via the producer's async callback at WARNING level so they don't
/// disappear silently. The Kafka `Future<RecordMetadata>` is otherwise discarded — callers who
/// need synchronous send semantics or precise failure handling per record should use
/// `KPipeProducer.send` / `sendAsync` directly rather than wiring this sink.
///
/// @param <T> The type of the processed object.
public class KafkaMessageSink<T> implements MessageSink<T> {

  private static final Logger LOGGER = System.getLogger(KafkaMessageSink.class.getName());

  private final Producer<byte[], byte[]> producer;
  private final String topic;
  private final Function<T, byte[]> keyMapper;
  private final Function<T, byte[]> valueMapper;

  /// Creates a new KafkaMessageSink.
  ///
  /// @param producer    the Kafka producer to use
  /// @param topic       the destination topic
  /// @param keyMapper   function to serialize the key (may be null for null keys)
  /// @param valueMapper function to serialize the value
  public KafkaMessageSink(
    final Producer<byte[], byte[]> producer,
    final String topic,
    final Function<T, byte[]> keyMapper,
    final Function<T, byte[]> valueMapper
  ) {
    this.producer = Objects.requireNonNull(producer, "producer cannot be null");
    this.topic = Objects.requireNonNull(topic, "topic cannot be null");
    this.keyMapper = keyMapper;
    this.valueMapper = Objects.requireNonNull(valueMapper, "valueMapper cannot be null");
  }

  @Override
  public void accept(final T value) {
    if (value == null) return;
    final var key = keyMapper != null ? keyMapper.apply(value) : null;
    final var val = valueMapper.apply(value);
    producer.send(new ProducerRecord<>(topic, key, val), (metadata, exception) -> {
      if (exception != null) LOGGER.log(Level.WARNING, "Failed to send record to topic %s".formatted(topic), exception);
    });
  }

  /// Creates a [KafkaMessageSink] that uses null for keys.
  ///
  /// @param producer    The Kafka producer to use.
  /// @param topic       The destination topic.
  /// @param valueMapper Function to serialize the value.
  /// @param <T>         The type of the processed object.
  /// @return A new KafkaMessageSink.
  public static <T> KafkaMessageSink<T> of(
    final Producer<byte[], byte[]> producer,
    final String topic,
    final Function<T, byte[]> valueMapper
  ) {
    return new KafkaMessageSink<>(producer, topic, null, valueMapper);
  }
}
