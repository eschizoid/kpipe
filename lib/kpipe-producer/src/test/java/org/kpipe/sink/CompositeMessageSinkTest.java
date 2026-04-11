package org.kpipe.sink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class CompositeMessageSinkTest {

  @Test
  @SuppressWarnings("unchecked")
  void shouldBroadcastToAllSinks() {
    // Given
    final var sink1 = (MessageSink<String>) mock(MessageSink.class);
    final var sink2 = (MessageSink<String>) mock(MessageSink.class);
    final var compositeSink = new CompositeMessageSink<>(List.of(sink1, sink2));
    final var processedValue = "processed";

    // When
    compositeSink.accept(processedValue);

    // Then
    verify(sink1).accept(processedValue);
    verify(sink2).accept(processedValue);
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldContinueOnSinkFailure() {
    // Given
    final var failingSink = (MessageSink<String>) mock(MessageSink.class);
    final var successfulSink = (MessageSink<String>) mock(MessageSink.class);

    doThrow(new RuntimeException("Sink failed")).when(failingSink).accept(any());

    final var compositeSink = new CompositeMessageSink<>(List.of(failingSink, successfulSink));
    final var processedValue = "processed";

    // When
    assertDoesNotThrow(() -> compositeSink.accept(processedValue));

    // Then
    verify(failingSink).accept(processedValue);
    verify(successfulSink).accept(processedValue);
  }
}
