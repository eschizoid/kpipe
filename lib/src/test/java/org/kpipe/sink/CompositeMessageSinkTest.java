package org.kpipe.sink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class CompositeMessageSinkTest {

  @Test
  @SuppressWarnings("unchecked")
  void shouldBroadcastToAllSinks() {
    // Given
    final var sink1 = (MessageSink<String, String>) mock(MessageSink.class);
    final var sink2 = (MessageSink<String, String>) mock(MessageSink.class);
    final var compositeSink = new CompositeMessageSink<>(List.of(sink1, sink2));
    final var record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");
    final var processedValue = "processed";

    // When
    compositeSink.send(record, processedValue);

    // Then
    verify(sink1).send(record, processedValue);
    verify(sink2).send(record, processedValue);
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldContinueOnSinkFailure() {
    // Given
    final var failingSink = (MessageSink<String, String>) mock(MessageSink.class);
    final var successfulSink = (MessageSink<String, String>) mock(MessageSink.class);

    doThrow(new RuntimeException("Sink failed")).when(failingSink).send(any(), any());

    final var compositeSink = new CompositeMessageSink<>(List.of(failingSink, successfulSink));
    final var record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");
    final var processedValue = "processed";

    // When
    assertDoesNotThrow(() -> compositeSink.send(record, processedValue));

    // Then
    verify(failingSink).send(record, processedValue);
    verify(successfulSink).send(record, processedValue);
  }
}
