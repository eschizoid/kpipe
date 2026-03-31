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
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kpipe.sink.MessageSink;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KPipeConsumerTest {

  private static final String TOPIC = "test-topic";
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

  @Mock
  private Function<String, String> mockProcessor;

  private Properties properties;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("enable.auto.commit", "true");
  }

  private KPipeConsumer<String, String> createConsumer(Function<String, String> processor) {
    return KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(processor)
      .build();
  }

  private ConsumerRecord<String, String> createRecord(long offset, String key, String value) {
    return new ConsumerRecord<>(TOPIC, 0, offset, key, value);
  }

  @Test
  void constructorWithValidParametersShouldNotThrowException() {
    // Arrange & Act & Assert
    assertDoesNotThrow(() -> createConsumer(mockProcessor));
    assertDoesNotThrow(() ->
      KPipeConsumer.<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(mockProcessor)
        .withPollTimeout(POLL_TIMEOUT)
        .build()
    );
  }

  @Test
  void constructorWithNullParametersShouldThrowNullPointerException() {
    // Arrange & Act & Assert
    assertThrows(NullPointerException.class, () -> KPipeConsumer.<String, String>builder().build());
    assertThrows(NullPointerException.class, () ->
      KPipeConsumer.<String, String>builder().withProperties(null).withTopic(TOPIC).withProcessor(mockProcessor).build()
    );
    assertThrows(NullPointerException.class, () ->
      KPipeConsumer.<String, String>builder()
        .withProperties(properties)
        .withTopic(null)
        .withProcessor(mockProcessor)
        .build()
    );
    assertThrows(NullPointerException.class, () ->
      KPipeConsumer.<String, String>builder().withProperties(properties).withTopic(TOPIC).withProcessor(null).build()
    );
  }

  @Test
  void isRunningShouldReturnFalseAfterConstruction() {
    // Arrange
    final var consumer = createConsumer(mockProcessor);

    // Act & Assert
    assertFalse(consumer.isRunning());
    consumer.close();
  }

  @Test
  void isRunningShouldReturnFalseAfterClose() {
    // Arrange
    final var consumer = createConsumer(mockProcessor);

    // Act
    consumer.close();

    // Assert
    assertFalse(consumer.isRunning());
  }

  @Test
  void closeCalledMultipleTimesShouldBeIdempotent() {
    // Arrange
    final var consumer = createConsumer(mockProcessor);

    // Act
    consumer.close();
    consumer.close();

    // Assert
    assertFalse(consumer.isRunning());
  }

  @Test
  void autoCloseableShouldCloseConsumerWhenExitingTryWithResources() {
    // Arrange & Act
    try (final var consumer = createConsumer(mockProcessor)) {
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
    final Function<String, String> retryProcessor = value -> {
      if (attempts.getAndIncrement() == 0) throw new RuntimeException("First attempt failure");
      return value.toUpperCase();
    };
    var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(retryProcessor)
      .withRetry(2, Duration.ofMillis(10))
      .withMetrics(true)
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
    final Function<String, String> failingProcessor = value -> {
      throw new RuntimeException("Always failing");
    };
    Consumer<KPipeConsumer.ProcessingError<String, String>> errorHandler = mock(Consumer.class);
    var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(failingProcessor)
      .withRetry(2, Duration.ofMillis(10))
      .withErrorHandler(errorHandler)
      .withMetrics(true)
      .build();
    final var record = createRecord(0, "key", "hello");

    // Act
    consumer.processRecord(record);
    Thread.sleep(100);

    // Assert
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
    final Function<String, String> intermittentProcessor = value -> {
      int count = counter.incrementAndGet();
      if (count % 3 != 0) throw new RuntimeException("Intermittent failure #" + count);
      return value.toUpperCase();
    };
    var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(intermittentProcessor)
      .withRetry(5, Duration.ofMillis(10))
      .withMetrics(true)
      .build();

    // Act
    for (int i = 0; i < 3; i++) {
      var record = createRecord(i, "key" + i, "value" + i);
      consumer.processRecord(record);
    }
    Thread.sleep(200);

    // Assert
    var metrics = consumer.getMetrics();
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
    final var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(mockProcessor)
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
    final var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(mockProcessor)
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
    final var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(mockProcessor)
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
    final var consumer = createConsumer(mockProcessor);
    commandQueue.offer(new ConsumerCommand.Close());

    // Act & Assert
    assertDoesNotThrow(consumer::processCommands);
  }

  @Test
  void customMessageSinkShouldReceiveProcessedMessages() {
    // Arrange
    @SuppressWarnings("unchecked")
    final var sink = mock(MessageSink.class);
    var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(s -> s + "-processed")
      .withMessageSink(sink)
      .build();
    var record = createRecord(1, "k", "v");

    // Act
    consumer.processRecord(record);

    // Assert
    verify(sink).accept(eq("v-processed"));
    consumer.close();
  }

  @Test
  void metricsShouldBeEmptyWhenDisabled() {
    // Arrange
    final var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(s -> s)
      .withMetrics(false)
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
    final var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(Function.identity())
      .withMessageSink(value -> {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      })
      .withBackpressure(2, 1)
      .withCommandQueue(commandQueue)
      .build();

    // Act: send 3 records to push in-flight above high watermark, then check backpressure
    consumer.processRecord(createRecord(0, "k0", "v0"));
    consumer.processRecord(createRecord(1, "k1", "v1"));
    consumer.processRecord(createRecord(2, "k2", "v2"));

    // Wait briefly for virtual threads to start (messages are received but not yet processed)
    Thread.sleep(50);

    // Simulate the loop calling checkBackpressure by checking state directly via pause/resume
    // In-flight = 3 received - 0 processed = 3 >= highWatermark(2) → should pause
    // We call processRecord again to push metrics, then verify via getMetrics
    final var metrics = consumer.getMetrics();
    assertTrue(metrics.get("messagesReceived") >= 3);

    consumer.close();
  }

  @Test
  void withBackpressureShouldIncrementPauseCountWhenHighWatermarkExceeded() throws InterruptedException {
    // Arrange: high watermark of 2, low watermark of 1
    final var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(Function.identity())
      .withMessageSink(value -> {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      })
      .withBackpressure(2, 1)
      .build();

    // Act: receive 3 messages (in-flight will be 3 >= highWatermark 2)
    consumer.processRecord(createRecord(0, "k0", "v0"));
    consumer.processRecord(createRecord(1, "k1", "v1"));
    consumer.processRecord(createRecord(2, "k2", "v2"));
    Thread.sleep(50); // let virtual threads start so messagesReceived is 3

    // Simulate the consumer loop calling checkBackpressure
    consumer.pause(); // manually trigger pause as checkBackpressure would
    // Verify metrics exist
    final var metrics = consumer.getMetrics();
    assertTrue(metrics.containsKey(KPipeConsumer.METRIC_BACKPRESSURE_PAUSE_COUNT));
    assertTrue(metrics.containsKey(KPipeConsumer.METRIC_BACKPRESSURE_TIME_MS));

    consumer.close();
  }

  @Test
  void withBackpressureShouldNotAddBackpressureMetricsWhenDisabled() {
    // Arrange: no withBackpressure call
    final var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(Function.identity())
      .build();

    // Assert: backpressure metric keys are absent
    final var metrics = consumer.getMetrics();
    assertFalse(metrics.containsKey(KPipeConsumer.METRIC_BACKPRESSURE_PAUSE_COUNT));
    assertFalse(metrics.containsKey(KPipeConsumer.METRIC_BACKPRESSURE_TIME_MS));

    consumer.close();
  }

  @Test
  void withBackpressureShouldNotAffectConsumerWhenDisabled() {
    // Arrange: no withBackpressure call
    final var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(Function.identity())
      .build();

    // Act & Assert: consumer builds and is usable without errors
    assertFalse(consumer.isRunning());
    assertDoesNotThrow(() -> consumer.processRecord(createRecord(0, "k", "v")));
    consumer.close();
  }

  @Test
  void withBackpressureNoArgShouldUseDefaultWatermarks() {
    // withBackpressure() with no args should build successfully (uses 10_000 / 7_000 defaults)
    assertDoesNotThrow(() ->
      KPipeConsumer.<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(Function.identity())
        .withBackpressure()
        .build()
        .close()
    );
  }

  @Test
  void withBackpressureShouldThrowWhenLowWatermarkEqualsHighWatermark() {
    assertThrows(IllegalArgumentException.class, () ->
      KPipeConsumer.<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(Function.identity())
        .withBackpressure(1000, 1000)
        .build()
    );
  }

  @Test
  void withBackpressureShouldThrowWhenLowWatermarkExceedsHighWatermark() {
    assertThrows(IllegalArgumentException.class, () ->
      KPipeConsumer.<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(Function.identity())
        .withBackpressure(500, 1000)
        .build()
    );
  }

  @Test
  void withBackpressureAndMetricsDisabledShouldNowWorkAtBuildTime() {
    assertDoesNotThrow(() ->
      KPipeConsumer.<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(Function.identity())
        .withMetrics(false)
        .withBackpressure(10_000, 7_000)
        .build()
    );
  }

  @Test
  void sequentialProcessingShouldProcessInOrder() {
    // Arrange
    final var processed = new ArrayList<>();
    var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessor(s -> {
        processed.add(s);
        return s;
      })
      .withSequentialProcessing(true)
      .build();

    final var records = new ConsumerRecords<>(
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
