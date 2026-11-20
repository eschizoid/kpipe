package io.github.eschizoid.kpipe.tracing;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class GuardedTracerTest {

  private static final ConsumerRecord<byte[], byte[]> RECORD = new ConsumerRecord<>("t", 0, 0L, null, new byte[0]);

  /// A span whose every call throws `failure`, recording which calls reached it.
  private static Tracer.SpanScope throwing(final List<String> calls, final Throwable failure) {
    return new Tracer.SpanScope() {
      @Override
      public Activation activate() {
        calls.add("activate");
        return () -> {
          calls.add("deactivate");
          throw sneaky(failure);
        };
      }

      @Override
      public void inject(final Headers headers) {
        calls.add("inject");
        throw sneaky(failure);
      }

      @Override
      public void attemptFailed(final int attempt, final Throwable cause) {
        calls.add("attemptFailed");
        throw sneaky(failure);
      }

      @Override
      public void recordException(final Throwable cause) {
        calls.add("recordException");
        throw sneaky(failure);
      }

      @Override
      public void close() {
        calls.add("close");
        throw sneaky(failure);
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> E sneaky(final Throwable failure) throws E {
    throw (E) failure;
  }

  @Test
  void everyCallReachesTheSpanAndNoneThrows() {
    final var calls = new ArrayList<String>();
    final var tracer = Tracer.guarded((_, _) -> throwing(calls, new AssertionError("broken")));

    final var span = tracer.startConsumerSpan(RECORD, "g");
    assertDoesNotThrow(() -> {
      try (var _ = span.activate()) {
        span.inject(new RecordHeaders());
        span.attemptFailed(0, new RuntimeException());
        span.recordException(new RuntimeException());
      }
      span.close();
    });
    assertEquals(List.of("activate", "inject", "attemptFailed", "recordException", "deactivate", "close"), calls);
  }

  @Test
  void aCheckedExceptionIsSwallowedToo() {
    final var calls = new ArrayList<String>();
    final var span = Tracer.guarded((_, _) -> throwing(calls, new Exception("checked"))).startConsumerSpan(
      RECORD,
      null
    );

    assertDoesNotThrow(() -> span.inject(new RecordHeaders()));
  }

  @Test
  void aTracerThatThrowsOrReturnsNullGivesTheNoopSpan() {
    final Tracer throwsOnStart = (_, _) -> {
      throw new IllegalStateException("down");
    };
    assertSame(Tracer.SpanScope.noop(), Tracer.guarded(throwsOnStart).startConsumerSpan(RECORD, null));
    assertSame(Tracer.SpanScope.noop(), Tracer.guarded((_, _) -> null).startConsumerSpan(RECORD, null));
  }

  @Test
  void aNullActivationBecomesNone() {
    final Tracer.SpanScope nullActivation = new Tracer.SpanScope() {
      @Override
      public Activation activate() {
        return null;
      }

      @Override
      public void inject(final Headers headers) {}

      @Override
      public void recordException(final Throwable failure) {}

      @Override
      public void close() {}
    };

    assertSame(
      Tracer.SpanScope.Activation.NONE,
      Tracer.guarded((_, _) -> nullActivation)
        .startConsumerSpan(RECORD, null)
        .activate()
    );
  }

  @Test
  void theJvmsFatalErrorsStillPassThrough() {
    final var span = Tracer.guarded((_, _) ->
      throwing(new ArrayList<>(), new OutOfMemoryError("fatal"))
    ).startConsumerSpan(RECORD, null);

    assertThrows(OutOfMemoryError.class, span::close);
    final Tracer fatalOnStart = (_, _) -> {
      throw new StackOverflowError();
    };
    assertThrows(StackOverflowError.class, () -> Tracer.guarded(fatalOnStart).startConsumerSpan(RECORD, null));
  }

  @Test
  void guardingIsIdempotentAndTheNoopTracerNeedsNone() {
    final Tracer tracer = (_, _) -> Tracer.SpanScope.noop();
    final var guarded = Tracer.guarded(tracer);

    assertSame(guarded, Tracer.guarded(guarded));
    assertSame(Tracer.noop(), Tracer.guarded(Tracer.noop()));
    assertSame(Tracer.noop(), Tracer.noop());
  }

  @Test
  void aWorkingSpanIsCalledThroughUnchanged() {
    final var calls = new ArrayList<String>();
    final Tracer.SpanScope working = new Tracer.SpanScope() {
      @Override
      public Activation activate() {
        calls.add("activate");
        return () -> calls.add("deactivate");
      }

      @Override
      public void inject(final Headers headers) {
        calls.add("inject");
        headers.add("x-span", new byte[] { 1 });
      }

      @Override
      public void attemptFailed(final int attempt, final Throwable cause) {
        calls.add("attemptFailed " + attempt);
      }

      @Override
      public void recordException(final Throwable cause) {
        calls.add("recordException " + cause.getMessage());
      }

      @Override
      public void close() {
        calls.add("close");
      }
    };
    final var span = Tracer.guarded((_, _) -> working).startConsumerSpan(RECORD, null);
    final var headers = new RecordHeaders();

    try (var _ = span.activate()) {
      span.inject(headers);
      span.attemptFailed(2, new RuntimeException());
      span.recordException(new RuntimeException("last"));
    }
    span.close();

    assertEquals(
      List.of("activate", "inject", "attemptFailed 2", "recordException last", "deactivate", "close"),
      calls
    );
    assertEquals(1, headers.lastHeader("x-span").value()[0]);
  }
}
