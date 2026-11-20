package io.github.eschizoid.kpipe.tracing;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class TraceContextTest {

  @Test
  void theBoundSpanIsCurrentOnlyInsideTheCall() throws Exception {
    final Tracer.SpanScope other = new Tracer.SpanScope() {
      @Override
      public Activation activate() {
        return Activation.NONE;
      }

      @Override
      public void inject(final Headers headers) {}

      @Override
      public void recordException(final Throwable failure) {}

      @Override
      public void close() {}
    };

    final var seen = TraceContext.callWith(other, TraceContext::current);

    assertSame(other, seen);
    assertSame(Tracer.SpanScope.noop(), TraceContext.current(), "nothing is bound outside the call");
  }

  @Test
  void theCallsResultAndExceptionPassThrough() {
    assertEquals(42, TraceContext.callWith(Tracer.SpanScope.noop(), () -> 42));
    assertThrows(IOException.class, () ->
      TraceContext.callWith(Tracer.SpanScope.noop(), () -> {
        throw new IOException("op");
      })
    );
    assertThrows(NullPointerException.class, () -> TraceContext.callWith(null, () -> 1));
  }

  @Test
  void theNoopSpanDoesNothing() {
    final var noop = Tracer.noop().startConsumerSpan(record(), "g");
    final var headers = new RecordHeaders();

    assertDoesNotThrow(() -> {
      try (var _ = noop.activate()) {
        noop.inject(headers);
        noop.attemptFailed(0, new RuntimeException());
        noop.recordException(new RuntimeException());
      }
      noop.close();
    });
    assertFalse(headers.iterator().hasNext());
  }

  private static ConsumerRecord<byte[], byte[]> record() {
    return new ConsumerRecord<>("t", 0, 0L, null, new byte[0]);
  }
}
