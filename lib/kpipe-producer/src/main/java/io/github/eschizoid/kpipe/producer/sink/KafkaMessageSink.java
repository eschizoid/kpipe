package io.github.eschizoid.kpipe.producer.sink;

import io.github.eschizoid.kpipe.producer.KPipeProducer;
import io.github.eschizoid.kpipe.record.RecordContext;
import io.github.eschizoid.kpipe.sink.MessageSink;
import io.github.eschizoid.kpipe.tracing.Tracer;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/// A [MessageSink] that sends processed messages to a Kafka topic.
///
/// Each output record carries the key and headers of the record being processed, read from
/// [RecordContext#current()]. With the default partitioner, records with the same non-null key go
/// to one output partition, and in `KEY_ORDERED` and `SEQUENTIAL` mode they arrive there in the
/// order they were consumed. When no record is bound, which is the case outside a pipeline, inside
/// a batch sink and on any other thread, the key is null and no headers are copied.
///
/// Each [#accept] waits for the broker to acknowledge the send; what an acknowledgement guarantees
/// depends on the producer's `acks` (Kafka's default, `all`, waits for the in-sync replicas). A
/// failed send throws, and the consumer handles it like any other sink failure: it retries the
/// record when retries are configured, then hands it to the error handler and, when configured,
/// the dead-letter topic, and counts it as failed. As one destination of a `CompositeMessageSink`,
/// a failure is logged and the record still succeeds.
///
/// Delivery is at least once. A send that times out may still have reached the broker, so its
/// retry can write the record twice; producer idempotence does not prevent that, because the retry
/// is a new send.
///
/// In SEQUENTIAL mode each record waits for its own ack, including the producer's `linger.ms`;
/// set `linger.ms=0` on the producer for such pipelines. PARALLEL overlaps sends across records,
/// and KEY_ORDERED across keys.
///
/// **Trace propagation.** A non-null `tracer` injects the active span's context (W3C `traceparent`
/// by default) into outbound record headers so downstream consumers see the same trace.
/// `OtelTracer` replaces any trace header copied from the input record; without a tracer, the
/// copied headers pass through unchanged. Pass `null` to disable injection — the sink coalesces to
/// an internal no-op tracer.
///
/// @param <T> The type of the processed object.
public final class KafkaMessageSink<T> implements MessageSink<T> {

  private static final Logger LOGGER = System.getLogger(KafkaMessageSink.class.getName());

  private final Function<ProducerRecord<byte[], byte[]>, Future<RecordMetadata>> send;
  private final String topic;
  private final Function<T, byte[]> valueMapper;
  private final Tracer tracer;

  private KafkaMessageSink(
    final Function<ProducerRecord<byte[], byte[]>, Future<RecordMetadata>> send,
    final String topic,
    final Function<T, byte[]> valueMapper,
    final Tracer tracer
  ) {
    this.send = send;
    this.topic = Objects.requireNonNull(topic, "topic cannot be null");
    this.valueMapper = Objects.requireNonNull(valueMapper, "valueMapper cannot be null");
    this.tracer = tracer != null ? tracer : Tracer.noop();
  }

  /// Creates a sink that sends through `producer`. The caller owns the producer and closes it.
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
    Objects.requireNonNull(producer, "producer cannot be null");
    return new KafkaMessageSink<>(producer::send, topic, valueMapper, tracer);
  }

  /// Creates a sink that sends through a [KPipeProducer]. The caller owns the producer and closes
  /// it.
  ///
  /// @param producer    the KPipe producer to use
  /// @param topic       the destination topic
  /// @param valueMapper function to serialize the value
  /// @param tracer      injects span context into outbound headers; pass `null` to disable
  /// @param <T>         the type of the processed object
  /// @return a new KafkaMessageSink
  public static <T> KafkaMessageSink<T> of(
    final KPipeProducer<byte[], byte[]> producer,
    final String topic,
    final Function<T, byte[]> valueMapper,
    final Tracer tracer
  ) {
    Objects.requireNonNull(producer, "producer cannot be null");
    return new KafkaMessageSink<>(producer::sendAsync, topic, valueMapper, tracer);
  }

  @Override
  public void accept(final T value) {
    if (value == null) return;
    final var context = RecordContext.isBound() ? RecordContext.current() : null;
    final var record = new ProducerRecord<>(topic, context != null ? context.key() : null, valueMapper.apply(value));
    if (context != null) context.forEachHeader(record.headers()::add);
    try {
      tracer.injectContextInto(record.headers());
    } catch (final Exception traceEx) {
      LOGGER.log(Level.WARNING, "Tracer.injectContextInto threw", traceEx);
    }
    try {
      send.apply(record).get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted waiting for the send to topic " + topic, e);
    } catch (final ExecutionException e) {
      throw new IllegalStateException("Failed to send record to topic " + topic, e.getCause());
    }
  }
}
