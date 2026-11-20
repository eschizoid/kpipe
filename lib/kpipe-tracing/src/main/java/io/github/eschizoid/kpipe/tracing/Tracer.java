package io.github.eschizoid.kpipe.tracing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

/// Traces records across the Kafka boundary. For each consumed record the consumer starts one span
/// with [#startConsumerSpan], continuing any trace the record's headers carry, and every record
/// it writes on that record's behalf, to the dead-letter topic or through a Kafka sink, carries
/// that span's context in its headers.
///
/// The default, [#noop()], traces nothing at no cost. `OtelTracer` in `kpipe-tracing-otel`
/// implements W3C trace-context propagation with OpenTelemetry; this module itself has no
/// OpenTelemetry dependency, so another tracing system can implement it.
///
/// An implementation must not throw: a failure to read or start a trace returns
/// [SpanScope#noop()]. The consumer guards every call anyway, so a tracer that throws only loses
/// its span and never fails a record.
@FunctionalInterface
public interface Tracer {
  /// Starts the span for processing `record`, as a child of the trace its headers carry, if any.
  ///
  /// @param record        the record being processed
  /// @param consumerGroup the consumer's `group.id`, or null when it has none
  /// @return the record's span; the consumer closes it once the record is done
  SpanScope startConsumerSpan(ConsumerRecord<?, byte[]> record, String consumerGroup);

  /// @return the tracer that traces nothing
  static Tracer noop() {
    return (_, _) -> SpanScope.noop();
  }

  /// One record's span. The consumer activates it on the record's thread while the record is
  /// processed, its retries included, records failures on it, and closes it once the record is
  /// done for good: when its sink returns, when it fails, or, on a batch route, when the batch that
  /// holds it is flushed. Close may therefore run on another thread than the one that started the
  /// span.
  interface SpanScope extends AutoCloseable {
    /// Makes this span current on the calling thread until the returned activation is closed, on
    /// the same thread, so instrumentation inside an operator or sink nests its own spans under
    /// it.
    ///
    /// @return the activation to close when the attempt ends
    Activation activate();

    /// Writes this span's context into `headers`, so a consumer of the written record continues
    /// the trace. Works on any thread, whether or not the span is active there.
    ///
    /// @param headers the outgoing record's headers
    void inject(Headers headers);

    /// Records one failed processing attempt. The record may still succeed on a retry.
    ///
    /// @param attempt the attempt, from 0
    /// @param failure why it failed
    default void attemptFailed(final int attempt, final Throwable failure) {}

    /// Records that the record failed for good, and marks the span as an error.
    ///
    /// @param failure why it failed
    void recordException(Throwable failure);

    /// Ends the span. Idempotent; may run on any thread.
    @Override
    void close();

    /// @return the span that records nothing
    static SpanScope noop() {
      return Noop.INSTANCE;
    }

    /// A span made current on one thread; closing it restores what was current before.
    @FunctionalInterface
    interface Activation extends AutoCloseable {
      /// The activation of a span that is never made current.
      Activation NONE = () -> {};

      @Override
      void close();
    }

    /// The span that records nothing, behind [SpanScope#noop()].
    enum Noop implements SpanScope {
      /// The only instance.
      INSTANCE;

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
    }
  }
}
