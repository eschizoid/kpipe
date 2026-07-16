package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import io.github.eschizoid.kpipe.metrics.ConsumerMetrics;
import io.github.eschizoid.kpipe.metrics.KPipeMetricsReporter;
import io.github.eschizoid.kpipe.producer.KPipeProducer;
import io.github.eschizoid.kpipe.producer.tracing.Tracer;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/// Pins null-argument validation on every public [KPipeConsumerBuilder] `with*` setter. Each
/// setter that mutates configuration must reject null at the call site rather than deferring the
/// failure to [KPipeConsumerBuilder#build] (where the NPE is harder to attribute) or to a later
/// runtime moment (where it crashes the consumer thread).
///
/// Also pins the blank-string rejection for `withDeadLetterTopic` — silently accepting an empty
/// or whitespace-only topic name would send DLQ writes to a misconfigured topic that the broker
/// then rejects per record, with no obvious source of the misconfig.
class KPipeConsumerBuilderValidationTest {

  private static KPipeConsumerBuilder builder() {
    return KPipeConsumer.builder();
  }

  private static void assertNpeWithMessage(final String expectedSubstring, final Executable action) {
    final var ex = assertThrows(NullPointerException.class, action);
    final var actual = ex.getMessage();
    assertEquals(
      true,
      actual != null && actual.contains(expectedSubstring),
      () -> "expected NPE message containing '" + expectedSubstring + "' but got: " + actual
    );
  }

  @Test
  void withPropertiesRejectsNull() {
    assertNpeWithMessage("props", () -> builder().withProperties(null));
  }

  @Test
  void withPipelineRejectsNull() {
    assertNpeWithMessage("pipeline", () -> builder().withPipeline(null));
  }

  @Test
  void withPollTimeoutRejectsNull() {
    assertNpeWithMessage("timeout", () -> builder().withPollTimeout(null));
  }

  @Test
  void withErrorHandlerRejectsNull() {
    assertNpeWithMessage("handler", () -> builder().withErrorHandler(null));
  }

  @Test
  void withDeadLetterTopicRejectsNull() {
    assertNpeWithMessage("topic", () -> builder().withDeadLetterTopic(null));
  }

  @Test
  void withDeadLetterTopicRejectsBlank() {
    final var b = builder();
    assertThrows(IllegalArgumentException.class, () -> b.withDeadLetterTopic(""));
    assertThrows(IllegalArgumentException.class, () -> b.withDeadLetterTopic("   "));
    assertThrows(IllegalArgumentException.class, () -> b.withDeadLetterTopic("\t\n"));
  }

  @Test
  void withKafkaProducerRawRejectsNull() {
    assertNpeWithMessage("producer", () -> builder().withKafkaProducer((Producer<byte[], byte[]>) null));
  }

  @Test
  void withRetryRejectsNullBackoff() {
    // Without this check, the NPE would surface on the worker virtual thread during retry
    // execution — exactly the deferred-failure mode the hygiene PR exists to close.
    assertNpeWithMessage("backoff", () -> builder().withRetry(3, null));
  }

  @Test
  void withMetricsRejectsNull() {
    assertNpeWithMessage("metrics", () -> builder().withMetrics(null));
  }

  @Test
  void withTracerRejectsNull() {
    assertNpeWithMessage("tracer", () -> builder().withTracer(null));
  }

  @Test
  void withMetricsReportersRejectsNull() {
    assertNpeWithMessage("reporters", () -> builder().withMetricsReporters(null));
  }

  @Test
  void withMetricsIntervalRejectsNull() {
    assertNpeWithMessage("interval", () -> builder().withMetricsInterval(null));
  }

  @Test
  void withCommandQueueRejectsNull() {
    assertNpeWithMessage("Command queue", () -> builder().withCommandQueue(null));
  }

  @Test
  void withOffsetManagerRejectsNull() {
    assertNpeWithMessage("OffsetManager", () -> builder().withOffsetManager(null));
  }

  @Test
  void withOffsetManagerProviderRejectsNull() {
    assertNpeWithMessage("provider", () -> builder().withOffsetManagerProvider(null));
  }

  @Test
  void withConsumerRejectsNull() {
    assertNpeWithMessage("provider", () -> builder().withConsumer(null));
  }

  // --- key.deserializer pinning ------------------------------------------------------------------

  /// Builds a consumer against a mock Kafka client and returns the properties `build()` mutated.
  /// Keys are always `byte[]` since 1.17.0, so `build()` must pin the key deserializer no matter
  /// what the caller configured — the old key-type-witness/deserializer mismatch surfaced as a
  /// `ClassCastException` deep inside record processing.
  private static Properties buildWithProps(final Properties props) {
    @SuppressWarnings("unchecked")
    final var pipeline = (MessagePipeline<String>) mock(MessagePipeline.class);
    try (
      final var consumer = builder()
        .withProperties(props)
        .withTopic("t")
        .withPipeline(pipeline)
        .withConsumer(() -> mock(Consumer.class))
        .build()
    ) {
      assertEquals(false, consumer == null);
    }
    return props;
  }

  @Test
  void buildPinsKeyDeserializerOverConflictingUserValue() {
    final var props = new Properties();
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    assertEquals(
      "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      buildWithProps(props).get("key.deserializer")
    );
  }

  @Test
  void buildPinsKeyDeserializerWhenUnset() {
    assertEquals(
      "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      buildWithProps(new Properties()).get("key.deserializer")
    );
  }

  // --- happy path: each setter accepts a non-null value without throwing ------------------------

  @Test
  void allSettersAcceptNonNullValues() {
    @SuppressWarnings("unchecked")
    final var pipeline = (MessagePipeline<String>) mock(MessagePipeline.class);
    @SuppressWarnings("unchecked")
    final var producer = (Producer<byte[], byte[]>) mock(Producer.class);
    @SuppressWarnings("unchecked")
    final var kpipeProducer = (KPipeProducer<byte[], byte[]>) mock(KPipeProducer.class);

    final var props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "test-group");
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    // None of these should throw. We don't call build() — that's covered by other tests.
    builder()
      .withProperties(props)
      .withPipeline(pipeline)
      .withPollTimeout(Duration.ofMillis(100))
      .withErrorHandler(_ -> {})
      .withDeadLetterTopic("dlq")
      .withKafkaProducer(producer)
      .withKafkaProducer(kpipeProducer)
      .withMetrics(ConsumerMetrics.noop())
      .withTracer(Tracer.noop())
      .withMetricsReporters(List.<KPipeMetricsReporter>of())
      .withMetricsInterval(Duration.ofSeconds(30))
      .withCommandQueue(new ConcurrentLinkedQueue<>())
      .withConsumer(() -> mock(Consumer.class));
  }
}
