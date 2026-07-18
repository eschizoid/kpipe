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
  private final ProducerMetrics metrics;
  private final Tracer tracer;

  /// Creates a new KPipeProducer that wraps an existing Kafka producer.
  ///
  /// @param producer    the Kafka producer to wrap
  /// @param ownProducer whether this wrapper owns the producer and should close it
  /// @param metrics the producer metrics — any [ProducerMetrics] impl (e.g. `OtelProducerMetrics`
  ///                from `kpipe-metrics-otel`); uses noop if null
  /// @param tracer      the tracer for outbound header injection; uses noop if null
  KPipeProducer(
    final Producer<K, V> producer,
    final boolean ownProducer,
    final ProducerMetrics metrics,
    final Tracer tracer
  ) {
    this.producer = Objects.requireNonNull(producer, "Producer cannot be null");
    this.ownProducer = ownProducer;
    // Guarded once at the source: a throwing user metrics implementation must never turn a
    // successful send/DLQ-park into a reported failure.
    this.metrics = GuardedProducerMetrics.guard(metrics);
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
  /// Either [#withProducer(Producer)] or [#withProperties(Properties)] must be
  /// called before [#build()]. When properties are provided, only the connection, security,
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
    /// All properties are forwarded as-is. `ByteArraySerializer` is set for key and
    /// value unless explicit serializers are already present. If a `client.id` is present,
    /// `"-producer"` is appended to distinguish the producer from the consumer.
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

    /// Sets the [ProducerMetrics] for this producer. The SPI is vendor-neutral (§10 "bring your own
    /// SDK"): use `io.github.eschizoid.kpipe.metrics.otel.OtelProducerMetrics` from
    /// `kpipe-metrics-otel` for an OpenTelemetry-backed instance, or [ProducerMetrics#noop()] for a
    /// no-op default.
    ///
    /// @param metrics the producer metrics
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
    /// @throws NullPointerException if neither [#withProducer(Producer)] nor
    ///     [#withProperties(Properties)] was called
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
  /// The DLQ record carries the original key, value, and ALL original headers, plus the
  /// `x-dlq-*` envelope (exception class/message, source topic/partition/offset/timestamp) and the
  /// current trace context. Original and envelope headers coexist; read `x-dlq-*` keys by last
  /// occurrence if a source header happens to share a name.
  ///
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
    // Preserve the ORIGINAL record's headers first (correlation ids, tenant markers — whatever the
    // producer attached): an operator replaying from the DLQ needs them, and dropping them made
    // DLQ records unrecoverable for root-cause analysis. The x-dlq-* envelope is appended after,
    // so both coexist (Kafka headers allow duplicate keys; DLQ consumers should read x-dlq-* by
    // last occurrence).
    for (final var header : record.headers()) {
      producerRecord.headers().add(header);
    }
    producerRecord.headers().add("x-dlq-exception-class", exception.getClass().getName().getBytes());
    producerRecord
      .headers()
      .add("x-dlq-exception-message", (exception.getMessage() != null ? exception.getMessage() : "").getBytes());
    producerRecord.headers().add("x-dlq-source-topic", sourceTopic.getBytes());
    producerRecord.headers().add("x-dlq-source-partition", String.valueOf(record.partition()).getBytes());
    producerRecord.headers().add("x-dlq-source-offset", String.valueOf(record.offset()).getBytes());
    // The DLQ record's own Kafka timestamp is the (meaningful) park time; the original event time
    // travels as a header so replay tooling can restore it.
    producerRecord.headers().add("x-dlq-source-timestamp", String.valueOf(record.timestamp()).getBytes());

    // Inject the current trace context (e.g. W3C `traceparent`) so DLQ subscribers can continue
    // the trace. Guarded — a misbehaving tracer must not break the error path.
    try {
      tracer.injectContextInto(producerRecord.headers());
    } catch (final Exception traceEx) {
      LOGGER.log(Level.WARNING, "Tracer.injectContextInto threw during DLQ send", traceEx);
    }

    try {
      send(producerRecord);
      metrics.recordDlqSent();
      LOGGER.log(Level.DEBUG, "Sent record to DLQ topic {0}", dlqTopic);
      return true;
    } catch (final Exception ex) {
      metrics.recordDlqFailed();
      LOGGER.log(Level.ERROR, () -> "Failed to send record to DLQ topic " + dlqTopic, ex);
      return false;
    }
  }

  /// Sends a record to a Kafka topic synchronously and returns the resulting metadata.
  ///
  /// When called from a virtual thread this is highly efficient — blocking on the future
  /// parks the virtual thread without pinning its carrier thread.
  ///
  /// The blocking wait is bounded by the producer's own `delivery.timeout.ms` (default 2 minutes):
  /// the Kafka client fails the returned future after that, so this call cannot park forever even
  /// against a hung broker. Tune that producer property to tighten or relax the bound.
  ///
  /// @param record the record to send
  /// @return the metadata for the record that was sent
  /// @throws RuntimeException if the send is interrupted or fails
  public RecordMetadata send(final ProducerRecord<K, V> record) {
    try {
      final var metadata = producer.send(record).get();
      metrics.recordMessageSent();
      return metadata;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      metrics.recordMessageFailed();
      throw new RuntimeException("Send interrupted", e);
    } catch (final ExecutionException e) {
      metrics.recordMessageFailed();
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
      if (exception == null) metrics.recordMessageSent();
      else metrics.recordMessageFailed();
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
