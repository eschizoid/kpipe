package org.kpipe.producer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/// A functional wrapper around a Kafka [Producer] used by KPipe for DLQ and output sinks.
///
/// @param <K> the type of keys in the produced records
/// @param <V> the type of values in the produced records
public class KPipeProducer<K, V> implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(KPipeProducer.class.getName());

  private final Producer<K, V> producer;
  private final boolean ownProducer;

  /// Creates a new KPipeProducer that wraps an existing Kafka producer.
  ///
  /// @param producer    the Kafka producer to wrap
  /// @param ownProducer whether this wrapper owns the producer and should close it
  KPipeProducer(final Producer<K, V> producer, final boolean ownProducer) {
    this.producer = Objects.requireNonNull(producer, "Producer cannot be null");
    this.ownProducer = ownProducer;
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
  /// <p>Either {@link #withProducer(Producer)} or {@link #withProperties(Properties)} must be
  /// called before {@link #build()}. When properties are provided, only the connection, security,
  /// and serialization keys are forwarded to the underlying Kafka producer.
  ///
  /// @param <K> the type of keys in the produced records
  /// @param <V> the type of values in the produced records
  public static class Builder<K, V> {

    private Builder() {}

    private Properties props;
    private Producer<K, V> producer;

    /// Sets the properties used to create the underlying Kafka producer.
    ///
    /// <p>All properties are forwarded as-is. {@code ByteArraySerializer} is set for key and
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

    /// Builds a new [KPipeProducer].
    ///
    /// @return a new KPipeProducer instance
    /// @throws NullPointerException if neither {@link #withProducer(Producer)} nor
    ///     {@link #withProperties(Properties)} was called
    public KPipeProducer<K, V> build() {
      if (producer != null) {
        return new KPipeProducer<>(producer, false);
      }
      Objects.requireNonNull(props, "Either withProducer or withProperties must be called");
      final var producerProps = new Properties(props);
      if (producerProps.containsKey("client.id")) producerProps.setProperty(
        "client.id",
        producerProps.getProperty("client.id") + "-producer"
      );
      producerProps.putIfAbsent("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      producerProps.putIfAbsent("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      return new KPipeProducer<>(new KafkaProducer<>(producerProps), true);
    }
  }

  /// Sends a consumer record that failed processing to a dead-letter topic.
  ///
  /// <p>This method synchronously sends the record to Kafka with enrichment headers containing
  /// information about the original topic, partition, offset, and error. The send is synchronous
  /// to ensure reliability in the error path — when called from a virtual thread this does not
  /// block the underlying carrier thread.
  ///
  /// @param dlqTopic    the name of the dead-letter topic
  /// @param record      the original consumer record that failed
  /// @param sourceTopic the original source topic name
  /// @param exception   the exception that caused the processing failure
  /// @param dlqMetric   optional metric counter to increment on successful DLQ send
  public void sendToDlq(
    final String dlqTopic,
    final ConsumerRecord<K, V> record,
    final String sourceTopic,
    final Exception exception,
    final AtomicLong dlqMetric
  ) {
    if (dlqTopic == null) return;

    final var producerRecord = new ProducerRecord<>(dlqTopic, record.key(), record.value());
    producerRecord.headers().add("x-dlq-exception-class", exception.getClass().getName().getBytes());
    producerRecord
      .headers()
      .add("x-dlq-exception-message", (exception.getMessage() != null ? exception.getMessage() : "").getBytes());
    producerRecord.headers().add("x-dlq-source-topic", sourceTopic.getBytes());
    producerRecord.headers().add("x-dlq-source-partition", String.valueOf(record.partition()).getBytes());
    producerRecord.headers().add("x-dlq-source-offset", String.valueOf(record.offset()).getBytes());

    try {
      send(producerRecord);
      if (dlqMetric != null) dlqMetric.incrementAndGet();
      LOGGER.log(Level.INFO, "Sent record to DLQ topic {0}", dlqTopic);
    } catch (final Exception ex) {
      LOGGER.log(Level.ERROR, "Failed to send record to DLQ topic " + dlqTopic, ex);
    }
  }

  /// Sends a record to a Kafka topic synchronously and returns the resulting metadata.
  ///
  /// <p>When called from a virtual thread this is highly efficient — blocking on the future
  /// parks the virtual thread without pinning its carrier thread.
  ///
  /// @param record the record to send
  /// @return the metadata for the record that was sent
  /// @throws RuntimeException if the send is interrupted or fails
  public RecordMetadata send(final ProducerRecord<K, V> record) {
    try {
      return producer.send(record).get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Send interrupted", e);
    } catch (final ExecutionException e) {
      throw new RuntimeException("Send failed", e.getCause());
    }
  }

  /// Sends a record to a Kafka topic asynchronously.
  ///
  /// @param record the record to send
  /// @return a future that will contain the record metadata
  public Future<RecordMetadata> sendAsync(final ProducerRecord<K, V> record) {
    return producer.send(record);
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
