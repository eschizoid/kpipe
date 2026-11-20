package io.github.eschizoid.kpipe.tracing;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

/// The [Tracer#guarded(Tracer)] wrapper: every call to the tracer, its spans and their activations
/// runs behind one guard.
final class GuardedTracer implements Tracer {

  private static final Logger LOGGER = System.getLogger(Tracer.class.getName());

  static final Tracer NOOP = (_, _) -> SpanScope.noop();

  private final Tracer tracer;

  private GuardedTracer(final Tracer tracer) {
    this.tracer = tracer;
  }

  static Tracer of(final Tracer tracer) {
    Objects.requireNonNull(tracer, "tracer cannot be null");
    return tracer instanceof GuardedTracer || tracer == NOOP ? tracer : new GuardedTracer(tracer);
  }

  @Override
  public SpanScope startConsumerSpan(final ConsumerRecord<?, byte[]> record, final String consumerGroup) {
    final var span = guard("startConsumerSpan", () -> tracer.startConsumerSpan(record, consumerGroup));
    return span == null ? SpanScope.noop() : new GuardedSpan(span);
  }

  /// Runs `call`, logging and swallowing any failure except the JVM's own fatal errors.
  ///
  /// @return what `call` returned, or null when it threw
  private static <T> T guard(final String method, final Call<T> call) {
    try {
      return call.run();
    } catch (final VirtualMachineError fatal) {
      throw fatal;
    } catch (final Throwable failure) {
      LOGGER.log(Level.WARNING, "Tracer call " + method + " threw; the record's trace loses it", failure);
      return null;
    }
  }

  @FunctionalInterface
  private interface Call<T> {
    T run() throws Throwable;
  }

  private record GuardedSpan(SpanScope span) implements SpanScope {
    @Override
    public Activation activate() {
      final var activation = guard("SpanScope.activate", span::activate);
      if (activation == null) return Activation.NONE;
      return () ->
        guard("SpanScope.Activation.close", () -> {
          activation.close();
          return null;
        });
    }

    @Override
    public void inject(final Headers headers) {
      guard("SpanScope.inject", () -> {
        span.inject(headers);
        return null;
      });
    }

    @Override
    public void attemptFailed(final int attempt, final Throwable failure) {
      guard("SpanScope.attemptFailed", () -> {
        span.attemptFailed(attempt, failure);
        return null;
      });
    }

    @Override
    public void recordException(final Throwable failure) {
      guard("SpanScope.recordException", () -> {
        span.recordException(failure);
        return null;
      });
    }

    @Override
    public void close() {
      guard("SpanScope.close", () -> {
        span.close();
        return null;
      });
    }
  }
}
