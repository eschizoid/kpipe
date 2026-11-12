package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.KPipeConsumerBuilder;
import io.github.eschizoid.kpipe.producer.KPipeProducer;
import io.github.eschizoid.kpipe.producer.sink.KafkaMessageSink;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.sink.MessageSink;
import io.github.eschizoid.kpipe.tracing.Tracer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/// The terminal sink [Stream#toKafka] builds. Its producer is created when the consumer starts:
/// [#openAll] builds one producer from the consumer properties for every such sink, and the
/// consumer closes it after its last record. A sink belongs to one consumer at a time.
///
/// @param <T> the stream's value type
final class KafkaTopicSink<T> implements MessageSink<T> {

  private final String topic;
  private final MessageFormat<T> format;
  private final AtomicReference<MessageSink<T>> delegate = new AtomicReference<>();

  private KafkaTopicSink(final String topic, final MessageFormat<T> format) {
    this.topic = topic;
    this.format = format;
  }

  static <T> KafkaTopicSink<T> of(final String topic, final MessageFormat<T> format) {
    Objects.requireNonNull(topic, "topic cannot be null");
    if (topic.isBlank()) throw new IllegalArgumentException("topic cannot be blank");
    Objects.requireNonNull(format, "format cannot be null");
    if (format.isSchemaRegistryBacked()) throw new IllegalStateException(
      "toKafka(\"" +
        topic +
        "\") needs a format that can serialize, and a Schema-Registry-backed format " +
        "only reads. Use toCustom(KafkaMessageSink.of(producer, topic, serializer, tracer)) with a writer format."
    );
    return new KafkaTopicSink<>(topic, format);
  }

  String topic() {
    return topic;
  }

  @Override
  public void accept(final T value) {
    final var sink = delegate.get();
    if (sink == null) throw new IllegalStateException(
      "The toKafka(\"" + topic + "\") sink sends only once its consumer has started"
    );
    sink.accept(value);
  }

  private boolean tryOpen(final KPipeProducer<byte[], byte[]> producer, final Tracer tracer) {
    return delegate.compareAndSet(null, KafkaMessageSink.of(producer, topic, format::serialize, tracer));
  }

  private void reset() {
    delegate.set(null);
  }

  /// The producer [#openAll] built and the sinks it opened on it.
  record Opened(KPipeProducer<byte[], byte[]> producer, List<KafkaTopicSink<?>> sinks) {
    /// Closes the producer and frees the sinks, so a later start can open them again.
    void abandon() {
      for (final var sink : sinks) sink.reset();
      producer.close();
    }
  }

  /// Opens every [KafkaTopicSink] in `sinks` on one producer built from `props`, and hands the
  /// producer to `builder`. The consumer closes it after its last record and also writes its
  /// dead-letter records through it.
  ///
  /// @return what was opened, which [#closingOnFailure] abandons if building or starting the
  /// consumer fails; null when `sinks` holds no [KafkaTopicSink]
  /// @throws IllegalStateException when a consumer already uses one of the sinks
  static Opened openAll(
    final Collection<? extends MessageSink<?>> sinks,
    final Properties props,
    final Tracer tracer,
    final KPipeConsumerBuilder builder
  ) {
    final var kafkaSinks = sinks
      .stream()
      .filter(KafkaTopicSink.class::isInstance)
      .map(s -> (KafkaTopicSink<?>) s)
      .toList();
    if (kafkaSinks.isEmpty()) return null;
    final var producer = KPipeProducer.<byte[], byte[]>builder().withProperties(props).withTracer(tracer).build();
    final var opened = new ArrayList<KafkaTopicSink<?>>(kafkaSinks.size());
    for (final var sink : kafkaSinks) {
      if (!sink.tryOpen(producer, tracer)) {
        new Opened(producer, opened).abandon();
        throw new IllegalStateException(
          "The toKafka(\"" + sink.topic + "\") sink is already used by a consumer; build a new stream to start another"
        );
      }
      opened.add(sink);
    }
    builder.withKafkaProducer(producer);
    return new Opened(producer, List.copyOf(opened));
  }

  /// Builds and starts the consumer. If either fails, abandons `opened` so the producer does not
  /// leak and the sinks can be started again. A consumer that fails after taking the producer
  /// closes it too; closing it twice is harmless.
  static <C> C closingOnFailure(final Opened opened, final Supplier<C> build) {
    try {
      return build.get();
    } catch (final RuntimeException | Error e) {
      if (opened != null) opened.abandon();
      throw e;
    }
  }
}
