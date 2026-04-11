package org.kpipe.producer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.kpipe.producer.config.KafkaProducerConfig;

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
  public KPipeProducer(final Producer<K, V> producer, final boolean ownProducer) {
    this.producer = producer;
    this.ownProducer = ownProducer;
  }

  /// Sends a consumer record that failed processing to a dead-letter topic.
  ///
  /// <p>This method synchronously sends the record to Kafka with additional enrichment headers
  /// containing information about the original topic, partition, offset, and error.
  ///
  /// @param dlqTopic       the name of the dead-letter topic
  /// @param record         the original consumer record that failed
  /// @param sourceTopic    the original source topic name
  /// @param exception      the exception that caused the processing failure
  /// @param dlqMetric      optional metric to increment on success
  public void sendToDlq(
    final String dlqTopic,
    final ConsumerRecord<K, V> record,
    final String sourceTopic,
    final Exception exception,
    final AtomicLong dlqMetric
  ) {
    if (dlqTopic == null || producer == null) return;

    final var producerRecord = new ProducerRecord<>(dlqTopic, record.key(), record.value());
    producerRecord.headers().add("x-dlq-exception-class", exception.getClass().getName().getBytes());
    producerRecord
      .headers()
      .add("x-dlq-exception-message", (exception.getMessage() != null ? exception.getMessage() : "").getBytes());
    producerRecord.headers().add("x-dlq-source-topic", sourceTopic.getBytes());
    producerRecord.headers().add("x-dlq-source-partition", String.valueOf(record.partition()).getBytes());
    producerRecord.headers().add("x-dlq-source-offset", String.valueOf(record.offset()).getBytes());

    try {
      send(producerRecord); // Synchronous to ensure reliability in error path
      if (dlqMetric != null) dlqMetric.incrementAndGet();
      LOGGER.log(Level.INFO, "Successfully sent message to DLQ topic {0}", dlqTopic);
    } catch (final Exception ex) {
      LOGGER.log(Level.ERROR, "Failed to send message to DLQ topic " + dlqTopic, ex);
    }
  }

  /// Sends a record to a Kafka topic synchronously.
  ///
  /// <p>This method blocks the current thread until the send is complete and returns the
  /// [RecordMetadata]. When called from a virtual thread, it is highly efficient as it
  /// does not block the underlying carrier thread.
  ///
  /// @param record the record to send
  /// @return the metadata for the record that was sent
  /// @throws RuntimeException if the send fails
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

  /// Creates a default Kafka producer based on the provided configuration properties.
  ///
  /// <p>This method filters the provided properties to include only those relevant to a producer,
  /// such as bootstrap servers, security settings, and client identification.
  ///
  /// @param props the base configuration properties
  /// @param context a descriptive context for logging (e.g., "DLQ")
  /// @param targetTopic the target topic name for logging
  /// @return a new Kafka producer instance
  public static <K, V> Producer<K, V> createDefaultProducer(
    final Properties props,
    final String context,
    final String targetTopic
  ) {
    final var producerProps = new Properties();
    props.forEach((k, v) -> {
      final String key = k.toString();
      if (
        key.startsWith("bootstrap.servers") ||
        key.startsWith("sasl.") ||
        key.startsWith("security.") ||
        key.startsWith("ssl.") ||
        key.startsWith("client.id") ||
        key.equals("key.serializer") ||
        key.equals("value.serializer") ||
        key.equals("acks")
      ) {
        producerProps.put(k, v);
      }
    });

    // If client.id is set, append "-producer" to distinguish it
    final String clientId = producerProps.getProperty("client.id");
    if (clientId != null) {
      producerProps.setProperty("client.id", clientId + "-producer");
    }

    // Default to ByteArraySerializer if not specified and types are byte[]
    final var finalProps = KafkaProducerConfig.producerBuilder()
      .with(p -> {
        p.putAll(producerProps);
        return p;
      })
      .withByteArraySerializers() // Only sets if not already present in the builder's logic?
      // No, builder overrides.
      .build();

    // Re-apply original serializers if they were explicitly provided in producerProps
    if (producerProps.containsKey("key.serializer")) {
      finalProps.put("key.serializer", producerProps.get("key.serializer"));
    }
    if (producerProps.containsKey("value.serializer")) {
      finalProps.put("value.serializer", producerProps.get("value.serializer"));
    }

    LOGGER.log(Level.INFO, "Creating default Kafka producer for {0} on topic {1}", context, targetTopic);
    return new KafkaProducer<>(finalProps);
  }

  @Override
  public void close() {
    if (ownProducer && producer != null) {
      try {
        producer.close();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error closing Kafka producer", e);
      }
    }
  }

  /// Returns the underlying Kafka producer.
  ///
  /// @return the Kafka producer
  public Producer<K, V> getProducer() {
    return producer;
  }
}
