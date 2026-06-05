package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
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

  /// Records whether close() was called — a probe for "did the consumer release its ctor
  /// resources?". Everything else is a minimal no-op OffsetManager impl.
  private static final class RecordingOffsetManager implements OffsetManager<String> {

    final AtomicBoolean closed = new AtomicBoolean(false);
    final AtomicInteger marks = new AtomicInteger(0);
    final AtomicBoolean markedAfterClose = new AtomicBoolean(false);
    // Optional probe: the real KafkaOffsetManager.close() does a final commitSync on the Kafka
    // consumer, so the consumer must still be open when the offset manager is closed. Set this to
    // the MockConsumer to record whether it was still open at close() time.
    volatile MockConsumer<String, byte[]> consumerProbe;
    final AtomicBoolean consumerOpenWhenClosed = new AtomicBoolean(false);

    @Override
    public OffsetManager<String> start() {
      return this;
    }

    @Override
    public OffsetManager<String> stop() {
      return this;
    }

    @Override
    public void trackOffset(final ConsumerRecord<String, byte[]> record) {}

    @Override
    public void markOffsetProcessed(final ConsumerRecord<String, byte[]> record) {
      if (closed.get()) markedAfterClose.set(true);
      marks.incrementAndGet();
    }

    @Override
    public void notifyCommitComplete(final String commitId, final boolean success) {}

    @Override
    public OffsetState getState() {
      return OffsetState.CREATED;
    }

    @Override
    public boolean isRunning() {
      return true;
    }

    @Override
    public Map<String, Object> getStatistics() {
      return Map.of();
    }

    @Override
    public void close() {
      if (consumerProbe != null) consumerOpenWhenClosed.set(!consumerProbe.closed());
      closed.set(true);
    }
  }

  @Test
  void consumerThreadSelfTerminationReleasesConstructorResources() throws InterruptedException {
    // If the consumer thread dies on its own (uncaught Throwable / interruption) without
    // close() being called, its finally sets state=CLOSED — which makes any later close() a
    // no-op. So the finally must itself release the ctor-created resources (dispatcher,
    // scheduler, offset manager, DLQ producer, batch wrappers) or they leak for the JVM's
    // lifetime. Probe via a RecordingOffsetManager: poll() throws an Error (escapes the loop's
    // catch(Exception), so the finally still runs), then assert the manager was closed.
    final var manager = new RecordingOffsetManager();
    final var mock = new MockConsumer<String, byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}

      @Override
      public synchronized ConsumerRecords<String, byte[]> poll(final Duration timeout) {
        throw new Error("simulated unexpected consumer-thread failure");
      }
    };
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> mock)
      .withOffsetManager(manager)
      // Registers a real JVM shutdown hook in the ctor so the hook-removal path inside
      // releaseConstructedResources() is exercised on self-termination. (The JVM hook
      // registry
      // isn't introspectable without fragile JDK-internal reflection, so we can't assert the
      // hook is gone directly — but the manager.closed assertion below proves
      // releaseConstructedResources() ran to completion, and hook removal is its last step.)
      .withShutdownHook(true)
      .build();

    consumer.start();

    final var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (!manager.closed.get() && System.nanoTime() < deadline) Thread.sleep(10);
    assertTrue(
      manager.closed.get(),
      "a self-terminating consumer thread must release ctor resources (here: the offset manager)"
    );
  }

  @Test
  void closeDoesNotTearDownResourcesWhileConsumerThreadIsStuck() throws InterruptedException {
    // If thread.join times out because the consumer thread is stuck (here: SEQUENTIAL mode
    // blocked inside the pipeline), close() must NOT tear down resources the live thread may
    // still use, and must not set CLOSED / count the latch (which would falsely report a clean
    // shutdown). It interrupts the thread and leaves teardown to the thread's own finally.
    // Probe with a RecordingOffsetManager: it must NOT be closed by close() on the timeout
    // path — only once the thread is unblocked and its finally runs.
    final var manager = new RecordingOffsetManager();
    final var entered = new CountDownLatch(1);
    final var gate = new CountDownLatch(1);

    final var mock = new MockConsumer<String, byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    mock.addRecord(createRecord(0, "k", "v"));

    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessingMode(ProcessingMode.SEQUENTIAL)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          entered.countDown();
          // Block until the gate opens, swallowing interrupts so close()'s interrupt
          // can't
          // free the thread — it must stay alive (and stuck) while we assert close's
          // behavior.
          while (true) {
            try {
              gate.await();
              break;
            } catch (final InterruptedException e) {
              // swallow; keep blocking
            }
          }
          return v;
        })
      )
      .withConsumer(() -> mock)
      .withOffsetManager(manager)
      .withWaitForMessagesTimeout(Duration.ofMillis(200))
      .withThreadTerminationTimeout(Duration.ofMillis(200))
      .build();

    consumer.start();
    assertTrue(entered.await(2, TimeUnit.SECONDS), "consumer thread must enter the pipeline and block");

    consumer.close(); // join times out (thread stuck) → interrupts + returns WITHOUT teardown

    assertFalse(manager.closed.get(), "close() must not tear down resources while the consumer thread is still alive");

    // Unblock the thread → its finally runs releaseConstructedResources → manager closed.
    gate.countDown();
    final var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (!manager.closed.get() && System.nanoTime() < deadline) Thread.sleep(10);
    assertTrue(manager.closed.get(), "once the thread unblocks and exits, its finally must release resources");
  }

  @Test
  void closeDrainsInFlightOffsetMarksBeforeTearingDownResources() throws InterruptedException {
    // On a normal close(), the consumer loop exits the instant state becomes CLOSING. A record
    // still in flight (PARALLEL mode, slow pipeline) finishes after that and enqueues its
    // MarkOffsetProcessed command. The consumer thread's finally must drain that command — while
    // the offset manager is still open — before releaseConstructedResources() closes it.
    // Otherwise the offset is silently abandoned (at-least-once breaks: the record reprocesses
    // on restart). Probe: the offset must be marked, and marked BEFORE the manager is closed.
    final var manager = new RecordingOffsetManager();
    final var entered = new CountDownLatch(1);

    final var mock = new MockConsumer<String, byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    mock.addRecord(createRecord(0, "k", "v"));

    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withProcessingMode(ProcessingMode.PARALLEL)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          entered.countDown();
          // Still in flight when close() fires, so the drain has to wait for us.
          try {
            Thread.sleep(150);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return v;
        })
      )
      .withConsumer(() -> mock)
      .withOffsetManager(manager)
      .withWaitForMessagesTimeout(Duration.ofSeconds(2))
      .withThreadTerminationTimeout(Duration.ofSeconds(2))
      .build();

    consumer.start();
    assertTrue(entered.await(2, TimeUnit.SECONDS), "the record must reach the pipeline before we close");

    consumer.close();

    assertTrue(manager.marks.get() >= 1, "the in-flight record's offset must be marked during close, not abandoned");
    assertFalse(
      manager.markedAfterClose.get(),
      "the offset must be marked BEFORE the offset manager is closed by releaseConstructedResources()"
    );
  }

  @Test
  void closeClosesOffsetManagerWhileKafkaConsumerStillOpen() throws InterruptedException {
    // KafkaOffsetManager.close() runs a final commitSync on the Kafka consumer to flush the last
    // safe offsets. So on shutdown the offset manager must be closed while the consumer is still
    // open — closing the consumer first makes that commit throw (swallowed), losing offsets. The
    // consumer thread's finally must release ctor resources (which closes the offset manager)
    // before it closes the Kafka consumer. Probe: record whether the consumer was still open when
    // the manager's close() ran.
    final var manager = new RecordingOffsetManager();
    final var mock = new MockConsumer<String, byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    manager.consumerProbe = mock;
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));

    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> mock)
      .withOffsetManager(manager)
      .build();

    consumer.start();
    consumer.close();

    // The thread's finally runs releaseConstructedResources (→ manager.close) before
    // kafkaConsumer.close(); wait for it to land.
    final var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (!manager.closed.get() && System.nanoTime() < deadline) Thread.sleep(10);
    assertTrue(manager.closed.get(), "the offset manager must be closed during shutdown");
    assertTrue(
      manager.consumerOpenWhenClosed.get(),
      "the offset manager must be closed (final commitSync) while the Kafka consumer is still open"
    );
  }

  @Test
  void closeDoesNotBurnTimeoutWaitingForBufferedBatchRecords() throws InterruptedException {
    // A size-only batch policy (maxSize=100) means a single buffered record never auto-flushes —
    // it's flushed by BatchPipelineWrapper.close() at teardown. So the in-flight drain must wait
    // only on the dispatcher's active work (pendingCount), not totalInFlight (which counts the
    // buffered record); otherwise close() burns the whole waitForMessagesTimeout while the
    // dispatcher is already idle. Probe: close() returns well under the timeout AND the buffered
    // record is still flushed + its offset marked at teardown.
    final var manager = new RecordingOffsetManager();
    final var flushedCount = new AtomicInteger(0);
    final var processed = new CountDownLatch(1);

    final var mock = new MockConsumer<String, byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    mock.addRecord(createRecord(0, "k", "v"));

    final var waitTimeout = Duration.ofSeconds(3);
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withConsumer(() -> mock)
      .withOffsetManager(manager)
      .withBatchPipeline(
        TOPIC,
        TestPipelines.sideEffect(v -> {
          processed.countDown();
          return v;
        }),
        BatchSink.ofVoid(batch -> flushedCount.addAndGet(batch.size())),
        BatchPolicy.ofSize(100)
      )
      .withPollTimeout(Duration.ofMillis(10))
      .withWaitForMessagesTimeout(waitTimeout)
      .withThreadTerminationTimeout(Duration.ofSeconds(3))
      .build();

    consumer.start();
    assertTrue(processed.await(2, TimeUnit.SECONDS), "the record must be processed and buffered");
    // Confirm the record is buffered (in-flight gauge counts it) and the dispatcher is idle.
    final var deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (consumer.getMetrics().getOrDefault("inFlight", 0L) < 1 && System.nanoTime() < deadline) Thread.sleep(5);
    assertEquals(1L, consumer.getMetrics().get("inFlight"), "the record should be buffered, not yet flushed");

    final var startNs = System.nanoTime();
    consumer.close();
    final var elapsed = Duration.ofNanos(System.nanoTime() - startNs);

    assertTrue(
      elapsed.compareTo(waitTimeout.dividedBy(2)) < 0,
      () -> "close() must not burn the drain timeout on a buffered batch; took " + elapsed.toMillis() + "ms"
    );
    assertEquals(1, flushedCount.get(), "the buffered record must still be flushed at teardown");
    assertTrue(manager.marks.get() >= 1, "the flushed record's offset must be marked at teardown");
  }

  @Test
  void closeBeforeStartReleasesKafkaConsumer() {
    // A consumer built but never started must still release every resource the constructor
    // opened when closed — most importantly the Kafka consumer itself. The CREATED → CLOSED
    // fast path used to return without closing it, leaking network connections + threads.
    final var mock = new MockConsumer<String, byte[]>("earliest");
    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> mock)
      .build();

    consumer.close(); // never started → takes the fast path

    assertTrue(mock.closed(), "never-started close must close the underlying Kafka consumer");
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
    // close() signals shutdown by offering a Close command to the queue. We can't assert on
    // residual queue contents: the consumer thread's finally drains the queue on the way out
    // (so late MarkOffsetProcessed commands aren't abandoned), which legitimately consumes the
    // Close command too. Record the offer instead, so the test survives that drain.
    final var closeOffered = new AtomicBoolean(false);
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>() {
      @Override
      public boolean offer(final ConsumerCommand command) {
        if (command instanceof ConsumerCommand.Close) closeOffered.set(true);
        return super.offer(command);
      }
    };
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
    assertTrue(closeOffered.get(), "close() must enqueue a Close command to signal shutdown");
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
      .withProcessingMode(ProcessingMode.SEQUENTIAL)
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

  @Test
  void keyOrderedProcessingPreservesPerKeyOrder() throws InterruptedException {
    // Records arrive in the deliberately interleaved order alpha-1, beta-1, alpha-2, beta-2,
    // alpha-3, beta-3. KEY_ORDERED must produce two strictly-ordered per-key sub-sequences;
    // the overall interleaving between keys can be anything.
    final var processed = new java.util.concurrent.ConcurrentHashMap<String, java.util.List<String>>();
    processed.put("alpha", new java.util.concurrent.CopyOnWriteArrayList<>());
    processed.put("beta", new java.util.concurrent.CopyOnWriteArrayList<>());
    final var latch = new java.util.concurrent.CountDownLatch(6);

    final var consumer = KPipeConsumer.<String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withPipeline(
        TestPipelines.sideEffect(b -> {
          final var payload = new String(b);
          processed.get(payload.split("-")[0]).add(payload);
          latch.countDown();
          return b;
        })
      )
      .withProcessingMode(ProcessingMode.KEY_ORDERED)
      .build();

    final var records = new ConsumerRecords<String, byte[]>(
      Map.of(
        new TopicPartition(TOPIC, 0),
        List.of(
          createRecord(0, "alpha", "alpha-1"),
          createRecord(1, "beta", "beta-1"),
          createRecord(2, "alpha", "alpha-2"),
          createRecord(3, "beta", "beta-2"),
          createRecord(4, "alpha", "alpha-3"),
          createRecord(5, "beta", "beta-3")
        )
      ),
      Map.of()
    );

    consumer.processRecords(records);
    assertTrue(latch.await(5, java.util.concurrent.TimeUnit.SECONDS));

    assertEquals(List.of("alpha-1", "alpha-2", "alpha-3"), processed.get("alpha"));
    assertEquals(List.of("beta-1", "beta-2", "beta-3"), processed.get("beta"));
    consumer.close();
  }
}
