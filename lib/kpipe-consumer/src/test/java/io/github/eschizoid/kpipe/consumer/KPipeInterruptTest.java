package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.github.eschizoid.kpipe.sink.MessageSink;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KPipeInterruptTest {

  @Mock
  private Function<byte[], byte[]> processor;

  @Mock
  private KafkaConsumer<byte[], byte[]> mockConsumer;

  @Mock
  private MessageSink<byte[]> messageSink;

  @Mock
  private KPipeConsumer.ErrorHandler errorHandler;

  @Mock
  private KafkaOffsetManager offsetManager;

  private KPipeConsumer createConsumer(
    final Queue<ConsumerCommand> commandQueue,
    final int maxRetries,
    final Duration backoff
  ) {
    final var props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "test-group");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    return KPipeConsumer.builder()
      .withProperties(props)
      .withTopic("test-topic")
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          final var result = processor.apply(value);
          messageSink.accept(result);
          return result;
        })
      )
      .withRetry(maxRetries, backoff)
      .withErrorHandler(errorHandler)
      .withCommandQueue(commandQueue)
      .withOffsetManager(offsetManager)
      .withConsumer(() -> mockConsumer)
      .build();
  }

  @Test
  void interruptDuringRetryShouldNotMarkOffsetAsProcessed() throws Exception {
    final var topic = "test-topic";
    final var value = "value".getBytes();
    final var record = new ConsumerRecord<>(topic, 0, 123L, "key".getBytes(UTF_8), value);
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();

    final var consumer = createConsumer(commandQueue, 1, Duration.ofMillis(1000));

    when(processor.apply(value)).thenThrow(new RuntimeException("first failure"));

    final var threadStarted = new CountDownLatch(1);
    final var threadFinished = new CountDownLatch(1);

    final var processingThread = Thread.ofVirtual().start(() -> {
      threadStarted.countDown();
      try {
        consumer.processRecord(record);
      } finally {
        threadFinished.countDown();
      }
    });

    assertTrue(threadStarted.await(1, TimeUnit.SECONDS));
    Thread.sleep(100); // let retry sleep begin
    processingThread.interrupt();
    assertTrue(threadFinished.await(1, TimeUnit.SECONDS));

    verify(messageSink, never()).accept(any());
    verify(errorHandler, never()).accept(any());
    verify(offsetManager, never()).markOffsetProcessed(any());
  }

  @Test
  void interruptionRelatedExceptionShouldNotMarkOffsetAsProcessed() throws Exception {
    final var topic = "test-topic";
    final var value = "value".getBytes();
    final var record = new ConsumerRecord<>(topic, 0, 456L, "key".getBytes(UTF_8), value);
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();
    final var consumer = createConsumer(commandQueue, 0, Duration.ofMillis(1));

    when(processor.apply(value)).thenThrow(new RuntimeException(new InterruptedException("interrupted")));

    final var interruptedFlag = new CompletableFuture<Boolean>();
    final var done = new CountDownLatch(1);

    Thread.ofVirtual().start(() -> {
      try {
        consumer.processRecord(record);
        interruptedFlag.complete(Thread.currentThread().isInterrupted());
      } finally {
        // clear interrupted status on this worker thread before it exits
        Thread.interrupted();
        done.countDown();
      }
    });

    assertTrue(done.await(1, TimeUnit.SECONDS));
    assertTrue(interruptedFlag.get(1, TimeUnit.SECONDS));

    verify(messageSink, never()).accept(any());
    verify(errorHandler, never()).accept(any());
    verify(offsetManager, never()).markOffsetProcessed(any());
  }

  @Test
  void terminalNonInterruptFailureShouldReportAndMarkOffset() {
    final var topic = "test-topic";
    final var value = "value".getBytes();
    final var record = new ConsumerRecord<>(topic, 0, 789L, "key".getBytes(UTF_8), value);
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();
    final var consumer = createConsumer(commandQueue, 0, Duration.ofMillis(1));

    when(processor.apply(value)).thenThrow(new RuntimeException("boom"));

    consumer.processRecord(record);

    verify(messageSink, never()).accept(any());
    verify(errorHandler, times(1)).accept(any());
    verify(offsetManager).markOffsetProcessed(record);
  }

  @Test
  void successShouldSendAndMarkOffset() {
    final var topic = "test-topic";
    final var value = "value".getBytes();
    final var processed = "processed".getBytes();
    final var record = new ConsumerRecord<>(topic, 0, 999L, "key".getBytes(UTF_8), value);
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();
    final var consumer = createConsumer(commandQueue, 0, Duration.ofMillis(1));

    when(processor.apply(value)).thenReturn(processed);

    consumer.processRecord(record);

    verify(messageSink, times(1)).accept(processed);
    verify(errorHandler, never()).accept(any());
    verify(offsetManager).markOffsetProcessed(record);
  }
}
