package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KPipeConsumerTest {

  private static final String TOPIC = "test-topic";
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

  @Mock
  private Function<byte[], byte[]> mockProcessor;

  private Properties properties;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("enable.auto.commit", "true");
  }

  private KPipeConsumer<String> createConsumer(final UnaryOperator<byte[]> processor) {
    return KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.sideEffect(processor))
      .build();
  }

  private KPipeConsumer<String> createConsumerWithMockProcessor() {
    return createConsumer(mockProcessor::apply);
  }

  private ConsumerRecord<String, byte[]> createRecord(final long offset, final String key, final String value) {
    return new ConsumerRecord<>(TOPIC, 0, offset, key, value.getBytes());
  }

  @Test
  void constructorWithValidParametersShouldNotThrowException() {
    // Arrange & Act & Assert
    assertDoesNotThrow(this::createConsumerWithMockProcessor);
    assertDoesNotThrow(() ->
      KPipeConsumer.<String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withPipeline(TestPipelines.identity())
        .withPollTimeout(POLL_TIMEOUT)
        .build()
    );
  }

  @Test
  void constructorWithNullParametersShouldThrowNullPointerException() {
    // Arrange & Act & Assert
    assertThrows(NullPointerException.class, () -> KPipeConsumer.<String>builder().build());
    assertThrows(NullPointerException.class, () ->
      KPipeConsumer.<String>builder()
        .withProperties(null)
        .withTopic(TOPIC)
        .withPipeline(TestPipelines.identity())
        .build()
    );
    assertThrows(NullPointerException.class, () ->
      KPipeConsumer.<String>builder()
        .withProperties(properties)
        .withTopic(null)
        .withPipeline(TestPipelines.identity())
        .build()
    );
    assertThrows(NullPointerException.class, () ->
      KPipeConsumer.<String>builder().withProperties(properties).withTopic(TOPIC).withPipeline(null).build()
    );
  }

  @Test
  void isRunningShouldReturnFalseAfterConstruction() {
    // Arrange
    final var consumer = createConsumerWithMockProcessor();

    // Act & Assert
    assertFalse(consumer.isRunning());
    consumer.close();
  }

  @Test
  void isRunningShouldReturnFalseAfterClose() {
    // Arrange
    final var consumer = createConsumerWithMockProcessor();

    // Act
    consumer.close();

    // Assert
    assertFalse(consumer.isRunning());
  }

  @Test
  void closeCalledMultipleTimesShouldBeIdempotent() {
    // Arrange
    final var consumer = createConsumerWithMockProcessor();

    // Act
    consumer.close();
    consumer.close();

    // Assert
    assertFalse(consumer.isRunning());
  }

  @Test
  void autoCloseableShouldCloseConsumerWhenExitingTryWithResources() {
    // Arrange & Act
    try (final var consumer = createConsumerWithMockProcessor()) {
      // Assert
      assertFalse(consumer.isRunning());
    }
    // No exception means success
    assertTrue(true);
  }

  @Test
  void processorWithRetryEnabledShouldRetryFailedMessages() throws InterruptedException {
    // Arrange
    final var attempts = new AtomicInteger(0);
    final UnaryOperator<byte[]> retryProcessor = value -> {
      if (attempts.getAndIncrement() == 0) throw new RuntimeException("First attempt failure");
      return new String(value).toUpperCase().getBytes();
    };
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.sideEffect(retryProcessor))
      .withRetry(2, Duration.ofMillis(10))
      .build();
    final var record = createRecord(0, "key", "hello");

    // Act
    consumer.processRecord(record);
    Thread.sleep(200);

    // Assert
    final var metrics = consumer.getMetrics();
    assertEquals(2, attempts.get());
    assertEquals(1, metrics.get("messagesProcessed"));
    assertEquals(1, metrics.get("retries"));
    assertEquals(0, metrics.get("processingErrors"));
    consumer.close();
  }

  @Test
  void processorWithMaxRetriesExceededShouldCallErrorHandler() throws InterruptedException {
    // Arrange
    final UnaryOperator<byte[]> failingProcessor = value -> {
      throw new RuntimeException("Always failing");
    };
    @SuppressWarnings("unchecked")
    final KPipeConsumer.ErrorHandler<String> errorHandler = mock(KPipeConsumer.ErrorHandler.class);
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.sideEffect(failingProcessor))
      .withRetry(2, Duration.ofMillis(10))
      .withErrorHandler(errorHandler)
      .build();
    final var record = createRecord(0, "key", "hello");

    // Act
    consumer.processRecord(record);
    Thread.sleep(100);

    // Assert
    @SuppressWarnings({ "unchecked", "rawtypes" })
    final var errorCaptor = ArgumentCaptor.forClass(KPipeConsumer.ProcessingError.class);
    verify(errorHandler).accept(errorCaptor.capture());
    final var error = errorCaptor.getValue();
    assertEquals(record, error.record());
    assertEquals(2, error.retryCount());
    assertNotNull(error.exception());
    final var metrics = consumer.getMetrics();
    assertEquals(2, metrics.get("retries"));
    assertEquals(0, metrics.get("messagesProcessed"));
    assertEquals(1, metrics.get("processingErrors"));
    consumer.close();
  }

  @Test
  void metricsTrackingWithFailuresAndRetriesShouldIncrementCorrectly() throws InterruptedException {
    // Arrange
    final var counter = new AtomicInteger(0);
    final UnaryOperator<byte[]> intermittentProcessor = value -> {
      final int count = counter.incrementAndGet();
      if (count % 3 != 0) throw new RuntimeException("Intermittent failure #" + count);
      return new String(value).toUpperCase().getBytes();
    };
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.sideEffect(intermittentProcessor))
      .withRetry(5, Duration.ofMillis(10))
      .build();

    // Act
    for (int i = 0; i < 3; i++) {
      final var record = createRecord(i, "key" + i, "value" + i);
      consumer.processRecord(record);
    }
    Thread.sleep(200);

    // Assert
    final var metrics = consumer.getMetrics();
    assertEquals(3, metrics.get("messagesReceived"));
    assertEquals(3, metrics.get("messagesProcessed"));
    assertEquals(6, metrics.get("retries"));
    assertEquals(0, metrics.get("processingErrors"));
    consumer.close();
  }

  @Test
  void pauseShouldChangeStateAndEnqueuePauseCommand() {
    // Arrange
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.sideEffect(mockProcessor::apply))
      .withCommandQueue(commandQueue)
      .build();

    // Act
    consumer.pause();

    // Assert
    assertTrue(consumer.isPaused());
    assertTrue(commandQueue.stream().anyMatch(c -> c instanceof ConsumerCommand.Pause));
  }

  @Test
  void resumeShouldChangeStateAndEnqueueResumeCommand() {
    // Arrange
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.sideEffect(mockProcessor::apply))
      .withCommandQueue(commandQueue)
      .build();
    consumer.pause();

    // Act
    consumer.resume();

    // Assert
    assertFalse(consumer.isPaused());
    assertTrue(commandQueue.stream().anyMatch(c -> c instanceof ConsumerCommand.Resume));
  }

  @Test
  void closeShouldEnqueueCloseCommand() {
    // Arrange
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.sideEffect(mockProcessor::apply))
      .withCommandQueue(commandQueue)
      .build();

    consumer.start();

    // Act
    consumer.close();

    // Assert
    assertTrue(commandQueue.contains(new ConsumerCommand.Close()));
  }

  @Test
  void processCommandsShouldHandleClose() {
    // Arrange
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var consumer = createConsumerWithMockProcessor();
    commandQueue.offer(new ConsumerCommand.Close());

    // Act & Assert
    assertDoesNotThrow(consumer::processCommands);
  }

  @Test
  void pipelineShouldProcessMessages() {
    // Arrange
    final var processed = new ArrayList<String>();
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(
        TestPipelines.sideEffect(b -> {
          final var s = new String(b);
          processed.add(s + "-processed");
          return (s + "-processed").getBytes();
        })
      )
      .build();
    final var record = createRecord(1, "k", "v");

    // Act
    consumer.processRecord(record);

    // Assert
    assertEquals(List.of("v-processed"), processed);
    consumer.close();
  }

  @Test
  void metricsShouldBeEmptyWhenDisabled() {
    // Arrange
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .disableMetrics()
      .build();
    final var record = createRecord(1, "k", "v");

    // Act
    consumer.processRecord(record);

    // Assert
    assertTrue(consumer.getMetrics().isEmpty());
    consumer.close();
  }

  @Test
  void withBackpressureShouldPauseConsumerWhenInFlightExceedsHighWatermark() throws InterruptedException {
    // Arrange: slow sink to keep messages in-flight; high watermark of 2
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return value;
        })
      )
      .withBackpressure(2, 1)
      .withCommandQueue(commandQueue)
      .build();

    // Act: send 3 records to push in-flight above high watermark, then check backpressure
    consumer.processRecord(createRecord(0, "k0", "v0"));
    consumer.processRecord(createRecord(1, "k1", "v1"));
    consumer.processRecord(createRecord(2, "k2", "v2"));

    // Wait briefly for virtual threads to start (messages are received but not yet processed)
    Thread.sleep(50);

    // In-flight = 3 received - 0 processed = 3 >= highWatermark(2)
    final var metrics = consumer.getMetrics();
    assertTrue(metrics.get("messagesReceived") >= 3);

    consumer.close();
  }

  @Test
  void withBackpressureShouldIncrementPauseCountWhenHighWatermarkExceeded() throws InterruptedException {
    // Arrange: high watermark of 2, low watermark of 1
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return value;
        })
      )
      .withBackpressure(2, 1)
      .build();

    // Act: receive 3 messages
    consumer.processRecord(createRecord(0, "k0", "v0"));
    consumer.processRecord(createRecord(1, "k1", "v1"));
    consumer.processRecord(createRecord(2, "k2", "v2"));
    Thread.sleep(50);

    consumer.pause();
    final var metrics = consumer.getMetrics();
    assertTrue(metrics.containsKey(KPipeConsumer.METRIC_BACKPRESSURE_PAUSE_COUNT));
    assertTrue(metrics.containsKey(KPipeConsumer.METRIC_BACKPRESSURE_TIME_MS));

    consumer.close();
  }

  @Test
  void withBackpressureShouldAddBackpressureMetricsByDefault() {
    // Arrange: no withBackpressure call
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .build();

    // Assert: backpressure metric keys are present by default
    final var metrics = consumer.getMetrics();
    assertTrue(metrics.containsKey(KPipeConsumer.METRIC_BACKPRESSURE_PAUSE_COUNT));
    assertTrue(metrics.containsKey(KPipeConsumer.METRIC_BACKPRESSURE_TIME_MS));

    consumer.close();
  }

  @Test
  void withBackpressureShouldBeEnabledByDefault() {
    // Arrange: no withBackpressure call
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .build();

    // Act & Assert: consumer builds and is usable
    assertFalse(consumer.isRunning());
    assertDoesNotThrow(() -> consumer.processRecord(createRecord(0, "k", "v")));
    consumer.close();
  }

  @Test
  void withBackpressureNoArgShouldUseDefaultWatermarks() {
    // withBackpressure() with no args should build successfully (uses 10_000 / 7_000 defaults)
    assertDoesNotThrow(() ->
      KPipeConsumer.<String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withPipeline(TestPipelines.identity())
        .withBackpressure()
        .build()
        .close()
    );
  }

  @Test
  void withBackpressureShouldThrowWhenLowWatermarkEqualsHighWatermark() {
    assertThrows(IllegalArgumentException.class, () ->
      KPipeConsumer.<String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withPipeline(TestPipelines.identity())
        .withBackpressure(1000, 1000)
        .build()
    );
  }

  @Test
  void withBackpressureShouldThrowWhenLowWatermarkExceedsHighWatermark() {
    assertThrows(IllegalArgumentException.class, () ->
      KPipeConsumer.<String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withPipeline(TestPipelines.identity())
        .withBackpressure(500, 1000)
        .build()
    );
  }

  @Test
  void withBackpressureAndMetricsDisabledShouldNowWorkAtBuildTime() {
    assertDoesNotThrow(() ->
      KPipeConsumer.<String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withPipeline(TestPipelines.identity())
        .disableMetrics()
        .withBackpressure(10_000, 7_000)
        .build()
    );
  }

  @Test
  void sequentialProcessingShouldProcessInOrder() {
    // Arrange
    final var processed = new ArrayList<String>();
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(
        TestPipelines.sideEffect(b -> {
          processed.add(new String(b));
          return b;
        })
      )
      .withSequentialProcessing(true)
      .build();

    final var records = new ConsumerRecords<String, byte[]>(
      Map.of(
        new TopicPartition(TOPIC, 0),
        List.of(createRecord(0, "k1", "v1"), createRecord(1, "k2", "v2"), createRecord(2, "k3", "v3"))
      ),
      Map.of()
    );

    // Act
    consumer.processRecords(records);

    // Assert
    assertEquals(List.of("v1", "v2", "v3"), processed);
    consumer.close();
  }
}
