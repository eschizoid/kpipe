package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kpipe.consumer.enums.ConsumerCommand;
import org.kpipe.sink.MessageSink;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KPipeInterruptTest {

  @Mock
  private Function<String, String> processor;

  @Mock
  private KafkaConsumer<String, String> mockConsumer;

  @Mock
  private MessageSink<String, String> messageSink;

  @Mock
  private Consumer<KPipeConsumer.ProcessingError<String, String>> errorHandler;

  @Mock
  private OffsetManager<String, String> offsetManager;

  private KPipeConsumer<String, String> createConsumer(
    final String topic,
    final Queue<ConsumerCommand> commandQueue,
    final int maxRetries,
    final Duration backoff
  ) {
    final var props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "test-group");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    return KPipeConsumer
      .<String, String>builder()
      .withProperties(props)
      .withTopic(topic)
      .withProcessor(processor)
      .withMessageSink(messageSink)
      .withRetry(maxRetries, backoff)
      .withErrorHandler(errorHandler)
      .withCommandQueue(commandQueue)
      .withOffsetManager(offsetManager)
      .withConsumer(() -> mockConsumer)
      .build();
  }

  private static boolean hasMarkOffsetProcessed(final Queue<ConsumerCommand> commandQueue, final long offset) {
    return commandQueue
      .stream()
      .anyMatch(cmd ->
        cmd == ConsumerCommand.MARK_OFFSET_PROCESSED && cmd.getRecord() != null && cmd.getRecord().offset() == offset
      );
  }

  @Test
  void interruptDuringRetryShouldNotMarkOffsetAsProcessed() throws Exception {
    final var topic = "test-topic";
    final var record = new ConsumerRecord<>(topic, 0, 123L, "key", "value");
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();

    final var consumer = createConsumer(topic, commandQueue, 1, Duration.ofMillis(1000));

    when(processor.apply("value")).thenThrow(new RuntimeException("first failure"));

    final var threadStarted = new CountDownLatch(1);
    final var threadFinished = new CountDownLatch(1);

    final var processingThread = Thread
      .ofVirtual()
      .start(() -> {
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

    verify(messageSink, never()).send(any(), any());
    verify(errorHandler, never()).accept(any());
    assertFalse(hasMarkOffsetProcessed(commandQueue, 123L));
  }

  @Test
  void interruptionRelatedExceptionShouldNotMarkOffsetAsProcessed() throws Exception {
    final var topic = "test-topic";
    final var record = new ConsumerRecord<>(topic, 0, 456L, "key", "value");
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();
    final var consumer = createConsumer(topic, commandQueue, 0, Duration.ofMillis(1));

    when(processor.apply("value")).thenThrow(new RuntimeException(new InterruptedException("interrupted")));

    final var interruptedFlag = new CompletableFuture<Boolean>();
    final var done = new CountDownLatch(1);

    Thread
      .ofVirtual()
      .start(() -> {
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

    verify(messageSink, never()).send(any(), any());
    verify(errorHandler, never()).accept(any());
    assertFalse(hasMarkOffsetProcessed(commandQueue, 456L));
  }

  @Test
  void terminalNonInterruptFailureShouldReportAndMarkOffset() {
    final var topic = "test-topic";
    final var record = new ConsumerRecord<>(topic, 0, 789L, "key", "value");
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();
    final var consumer = createConsumer(topic, commandQueue, 0, Duration.ofMillis(1));

    when(processor.apply("value")).thenThrow(new RuntimeException("boom"));

    consumer.processRecord(record);

    verify(messageSink, never()).send(any(), any());
    verify(errorHandler, times(1)).accept(any());
    assertTrue(hasMarkOffsetProcessed(commandQueue, 789L));
  }

  @Test
  void successShouldSendAndMarkOffset() {
    final var topic = "test-topic";
    final var record = new ConsumerRecord<>(topic, 0, 999L, "key", "value");
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();
    final var consumer = createConsumer(topic, commandQueue, 0, Duration.ofMillis(1));

    when(processor.apply("value")).thenReturn("processed");

    consumer.processRecord(record);

    verify(messageSink, times(1)).send(record, "processed");
    verify(errorHandler, never()).accept(any());
    assertTrue(hasMarkOffsetProcessed(commandQueue, 999L));
  }
}
