package org.kpipe.producer.tracing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

/// Tracer SPI for KPipe — opt-in distributed tracing across the Kafka boundary.
///
/// A `Tracer` is responsible for two operations on the consumer side:
///
///   1. **Extract** a parent context from an inbound [ConsumerRecord]'s headers and **start a
///      span** that represents the work of processing the record.
///   2. **Inject** the active span's context into outbound [Headers] so downstream consumers (DLQ
///      subscribers, services that read records produced by
// [org.kpipe.producer.sink.KafkaMessageSink])
///      see the same trace.
///
/// This module ships no concrete implementation. The default [#noop()] is a zero-cost stub used
/// when tracing is not configured. Add the `kpipe-tracing-otel` module and pass
/// `new OtelTracer(openTelemetry, "my-pipeline")` to wire W3C `traceparent` propagation through
/// the OpenTelemetry API.
///
/// **Why this lives in `kpipe-producer`:** the SPI references Kafka's `Headers` and
/// `ConsumerRecord` types, so it must live in a module that already requires `kafka.clients`.
/// `kpipe-metrics` is interfaces-only and dep-free; widening it would regress that.
// `kpipe-consumer`
/// already `requires transitive org.kpipe.producer`, so the SPI is reachable from both injection
/// callsites (`KPipeProducer.sendToDlq`, `KafkaMessageSink`) and the span-start callsite
/// (`KPipeConsumer.processRecord`) without introducing new module edges.
///
/// **No OTel types in this SPI** — kept pure-Java so implementations can be swapped (B3, Datadog,
/// custom) without dragging an OTel API dependency onto every user's classpath.
public interface Tracer {
  /// Starts a span representing the processing of `record`. The implementation should extract any
  /// upstream span context from the record's headers (e.g. W3C `traceparent`) and use it as the
  /// parent. The returned [SpanScope] MUST be closed when processing completes — typically in a
  /// `finally` block.
  ///
  /// Implementations must never throw from this method on a parse failure; return a no-op scope
  /// instead. A throwing tracer must never crash the consumer thread.
  ///
  /// @param record the Kafka record being processed
  /// @return a scope that ends the span when closed
  SpanScope startConsumerSpan(ConsumerRecord<?, byte[]> record);

  /// Injects the currently-active span context into `headers` so a downstream consumer can pick
  /// up the same trace. Called by [org.kpipe.producer.KPipeProducer#sendToDlq] and the Kafka sink
  /// before each outbound `send`.
  ///
  /// @param headers the outbound record's headers
  void injectContextInto(Headers headers);

  /// Returns the no-op tracer used when tracing is not configured. Zero allocation, zero cost.
  ///
  /// @return a shared no-op `Tracer`
  static Tracer noop() {
    return NoopTracer.INSTANCE;
  }

  /// Scope handle returned by [#startConsumerSpan]. Closing the scope ends the span and detaches
  /// it from the current thread.
  interface SpanScope extends AutoCloseable {
    /// Records `t` as an exception on the active span and marks the span status as error.
    /// Implementations must never throw — a failing tracer must not crash the caller.
    ///
    /// @param t the exception to record
    void recordException(Throwable t);

    /// Ends the span and detaches it from the current thread. Idempotent — closing twice is a
    /// no-op.
    @Override
    void close();

    /// Returns the no-op scope. Useful for tracer implementations that decide a particular record
    /// does not warrant a span.
    static SpanScope noop() {
      return NoopSpanScope.INSTANCE;
    }
  }
}
