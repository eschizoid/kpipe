package io.github.eschizoid.kpipe.producer.sink;

import io.github.eschizoid.kpipe.sink.MessageSink;
import io.github.eschizoid.kpipe.tracing.Tracer;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/// A [MessageSink] that sends processed messages to a Kafka topic.
///
/// Each [#accept] waits for the broker to acknowledge the send, so the record's offset is marked
/// only once the message is durably written. A failed send throws, and the consumer treats it like
/// any other sink failure: it retries the record, then sends it to the error handler and the
/// dead-letter topic, and counts it as failed. In SEQUENTIAL mode each record therefore waits for
/// its ack; PARALLEL overlaps sends across records, and KEY_ORDERED across keys.
///
/// **Trace propagation.** A non-null `tracer` injects the active span's context (W3C `traceparent`
/// by default) into outbound record headers so downstream consumers see the same trace. Pass
/// `null` to disable injection — the sink coalesces to an internal no-op tracer.
///
/// @param <T> The type of the processed object.
public class KafkaMessageSink<T> implements MessageSink<T> {

  private static final Logger LOGGER = System.getLogger(KafkaMessageSink.class.getName());

  private final Producer<byte[], byte[]> producer;
  private final String topic;
  private final Function<T, byte[]> keyMapper;
  private final Function<T, byte[]> valueMapper;
  private final Tracer tracer;

  /// Creates a new KafkaMessageSink.
  ///
  /// @param producer    the Kafka producer to use
  /// @param topic       the destination topic
  /// @param keyMapper   function to serialize the key (may be null for null keys)
  /// @param valueMapper function to serialize the value
  /// @param tracer      injects span context into outbound headers; pass `null` to disable
  public KafkaMessageSink(
    final Producer<byte[], byte[]> producer,
    final String topic,
    final Function<T, byte[]> keyMapper,
    final Function<T, byte[]> valueMapper,
    final Tracer tracer
  ) {
    this.producer = Objects.requireNonNull(producer, "producer cannot be null");
    this.topic = Objects.requireNonNull(topic, "topic cannot be null");
    this.keyMapper = keyMapper;
    this.valueMapper = Objects.requireNonNull(valueMapper, "valueMapper cannot be null");
    this.tracer = tracer != null ? tracer : Tracer.noop();
  }

  @Override
  public void accept(final T value) {
    if (value == null) return;
    final var key = keyMapper != null ? keyMapper.apply(value) : null;
    final var val = valueMapper.apply(value);
    final var record = new ProducerRecord<>(topic, key, val);
    try {
      tracer.injectContextInto(record.headers());
    } catch (final Exception traceEx) {
      LOGGER.log(Level.WARNING, "Tracer.injectContextInto threw", traceEx);
    }
    try {
      producer.send(record).get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted waiting for the send to topic " + topic, e);
    } catch (final ExecutionException e) {
      throw new IllegalStateException("Failed to send record to topic " + topic, e.getCause());
    }
  }

  /// Creates a [KafkaMessageSink] that uses null for keys.
  ///
  /// @param producer    the Kafka producer to use
  /// @param topic       the destination topic
  /// @param valueMapper function to serialize the value
  /// @param tracer      injects span context into outbound headers; pass `null` to disable
  /// @param <T>         the type of the processed object
  /// @return a new KafkaMessageSink
  public static <T> KafkaMessageSink<T> of(
    final Producer<byte[], byte[]> producer,
    final String topic,
    final Function<T, byte[]> valueMapper,
    final Tracer tracer
  ) {
    return new KafkaMessageSink<>(producer, topic, null, valueMapper, tracer);
  }
}
