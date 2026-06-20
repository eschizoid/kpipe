package io.github.eschizoid.kpipe.producer;

import io.github.eschizoid.kpipe.metrics.ProducerMetrics;
import io.github.eschizoid.kpipe.producer.tracing.Tracer;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/// A functional wrapper around a Kafka [Producer] used by KPipe for DLQ and output sinks.
///
/// @param <K> the type of keys in the produced records
/// @param <V> the type of values in the produced records
public class KPipeProducer<K, V> implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(KPipeProducer.class.getName());

  private final Producer<K, V> producer;
  private final boolean ownProducer;
  private final ProducerMetrics otelMetrics;
  private final Tracer tracer;

  /// Creates a new KPipeProducer that wraps an existing Kafka producer.
  ///
  /// @param producer    the Kafka producer to wrap
  /// @param ownProducer whether this wrapper owns the producer and should close it
  /// @param otelMetrics OpenTelemetry metrics instruments; uses noop if null
  /// @param tracer      the tracer for outbound header injection; uses noop if null
  KPipeProducer(
    final Producer<K, V> producer,
    final boolean ownProducer,
    final ProducerMetrics otelMetrics,
    final Tracer tracer
  ) {
    this.producer = Objects.requireNonNull(producer, "Producer cannot be null");
    this.ownProducer = ownProducer;
    this.otelMetrics = otelMetrics != null ? otelMetrics : ProducerMetrics.noop();
    this.tracer = tracer != null ? tracer : Tracer.noop();
  }

  /// Creates a new builder for constructing [KPipeProducer] instances.
  ///
  /// @param <K> the type of keys in the produced records
  /// @param <V> the type of values in the produced records
  /// @return a new builder instance
  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  /// Builder for creating and configuring [KPipeProducer] instances.
  ///
  /// Either {@link #withProducer(Producer)} or {@link #withProperties(Properties)} must be
  /// called before {@link #build()}. When properties are provided, only the connection, security,
  /// and serialization keys are forwarded to the underlying Kafka producer.
  ///
  /// @param <K> the type of keys in the produced records
  /// @param <V> the type of values in the produced records
  public static class Builder<K, V> {

    private Builder() {}

    private Properties props;
    private Producer<K, V> producer;
    private ProducerMetrics metrics;
    private Tracer tracer;

    /// Sets the properties used to create the underlying Kafka producer.
    ///
    /// All properties are forwarded as-is. {@code ByteArraySerializer} is set for key and
    /// value unless explicit serializers are already present. If a {@code client.id} is present,
    /// {@code "-producer"} is appended to distinguish the producer from the consumer.
    ///
    /// @param props the Kafka producer properties (or consumer properties to derive from)
    /// @return this builder instance for method chaining
    public Builder<K, V> withProperties(final Properties props) {
      this.props = Objects.requireNonNull(props, "Properties cannot be null");
      return this;
    }

    /// Sets an existing Kafka producer to wrap. The wrapper does not own this producer and will
    /// not close it.
    ///
    /// @param producer the Kafka producer to wrap
    /// @return this builder instance for method chaining
    public Builder<K, V> withProducer(final Producer<K, V> producer) {
      this.producer = Objects.requireNonNull(producer, "Producer cannot be null");
      return this;
    }

    /// Sets the OpenTelemetry metrics instruments for this producer.
    ///
    /// Use `io.github.eschizoid.kpipe.metrics.otel.OtelProducerMetrics` to create an instrumented
    /// instance, or
    /// [ProducerMetrics#noop()] for a no-op default.
    ///
    /// @param metrics the producer metrics instruments
    /// @return this builder instance for method chaining
    public Builder<K, V> withMetrics(final ProducerMetrics metrics) {
      this.metrics = metrics;
      return this;
    }

    /// Sets the tracer used to inject the active span's context into outbound headers (DLQ and
    /// Kafka sinks). When not set, no propagation occurs (`Tracer.noop()`).
    ///
    /// @param tracer the tracer to use; pass `Tracer.noop()` to disable explicitly
    /// @return this builder instance for method chaining
    public Builder<K, V> withTracer(final Tracer tracer) {
      this.tracer = tracer;
      return this;
    }

    /// Builds a new [KPipeProducer].
    ///
    /// @return a new KPipeProducer instance
    /// @throws NullPointerException if neither {@link #withProducer(Producer)} nor
    ///     {@link #withProperties(Properties)} was called
    public KPipeProducer<K, V> build() {
      if (producer != null) return new KPipeProducer<>(producer, false, metrics, tracer);
      Objects.requireNonNull(props, "Either withProducer or withProperties must be called");
      return new KPipeProducer<>(new KafkaProducer<>(buildProducerProperties(props)), true, metrics, tracer);
    }

    /// Builds the effective producer Properties from a source `Properties`. Visible for testing.
    /// User-supplied serializers are preserved (clones the source so `putIfAbsent` actually
    /// sees the parent entries — `new Properties(parent)` does not, which would cause the
    /// defaults to silently shadow user serializers).
    static Properties buildProducerProperties(final Properties source) {
      final var producerProps = (Properties) source.clone();
      if (producerProps.containsKey("client.id")) producerProps.setProperty(
        "client.id",
        producerProps.getProperty("client.id") + "-producer"
      );
      producerProps.putIfAbsent("key.serializer", ByteArraySerializer.class.getName());
      producerProps.putIfAbsent("value.serializer", ByteArraySerializer.class.getName());
      return producerProps;
    }
  }

  /// Sends a consumer record that failed processing to a dead-letter topic.
  ///
  /// Synchronously sends the record to Kafka with enrichment headers containing the original
  /// topic, partition, offset, exception class, and exception message. The send is synchronous to
  /// ensure reliability in the error path — when called from a virtual thread this does not block
  /// the underlying carrier thread.
  ///
  /// @param dlqTopic    the name of the dead-letter topic; if null this method is a no-op and
  ///                    returns false
  /// @param record      the original consumer record that failed
  /// @param sourceTopic the original source topic name
  /// @param exception   the exception that caused the processing failure
  /// @return true if the record was successfully sent to the DLQ, false otherwise (DLQ disabled or
  ///         send failed). Callers can use the return value to update their own counters or
  ///         trigger fallback handling.
  public boolean sendToDlq(
    final String dlqTopic,
    final ConsumerRecord<K, V> record,
    final String sourceTopic,
    final Exception exception
  ) {
    if (dlqTopic == null) return false;

    final var producerRecord = new ProducerRecord<>(dlqTopic, record.key(), record.value());
    producerRecord.headers().add("x-dlq-exception-class", exception.getClass().getName().getBytes());
    producerRecord
      .headers()
      .add("x-dlq-exception-message", (exception.getMessage() != null ? exception.getMessage() : "").getBytes());
    producerRecord.headers().add("x-dlq-source-topic", sourceTopic.getBytes());
    producerRecord.headers().add("x-dlq-source-partition", String.valueOf(record.partition()).getBytes());
    producerRecord.headers().add("x-dlq-source-offset", String.valueOf(record.offset()).getBytes());

    // Inject the current trace context (e.g. W3C `traceparent`) so DLQ subscribers can continue
    // the trace. Guarded — a misbehaving tracer must not break the error path.
    try {
      tracer.injectContextInto(producerRecord.headers());
    } catch (final Exception traceEx) {
      LOGGER.log(Level.WARNING, "Tracer.injectContextInto threw during DLQ send", traceEx);
    }

    try {
      send(producerRecord);
      otelMetrics.recordDlqSent();
      LOGGER.log(Level.DEBUG, "Sent record to DLQ topic {0}", dlqTopic);
      return true;
    } catch (final Exception ex) {
      LOGGER.log(Level.ERROR, () -> "Failed to send record to DLQ topic " + dlqTopic, ex);
      return false;
    }
  }

  /// Sends a record to a Kafka topic synchronously and returns the resulting metadata.
  ///
  /// When called from a virtual thread this is highly efficient — blocking on the future
  /// parks the virtual thread without pinning its carrier thread.
  ///
  /// @param record the record to send
  /// @return the metadata for the record that was sent
  /// @throws RuntimeException if the send is interrupted or fails
  public RecordMetadata send(final ProducerRecord<K, V> record) {
    try {
      final var metadata = producer.send(record).get();
      otelMetrics.recordMessageSent();
      return metadata;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      otelMetrics.recordMessageFailed();
      throw new RuntimeException("Send interrupted", e);
    } catch (final ExecutionException e) {
      otelMetrics.recordMessageFailed();
      throw new RuntimeException("Send failed", e.getCause());
    }
  }

  /// Sends a record to a Kafka topic asynchronously.
  ///
  /// Metrics are updated via a callback when the send completes — success increments
  /// `kpipe.producer.messages.sent`, failure increments `kpipe.producer.messages.failed`.
  ///
  /// @param record the record to send
  /// @return a future that will contain the record metadata
  public Future<RecordMetadata> sendAsync(final ProducerRecord<K, V> record) {
    return producer.send(record, (_, exception) -> {
      if (exception == null) otelMetrics.recordMessageSent();
      else otelMetrics.recordMessageFailed();
    });
  }

  @Override
  public void close() {
    if (ownProducer) {
      try {
        producer.close();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error closing Kafka producer", e);
      }
    }
  }
}
