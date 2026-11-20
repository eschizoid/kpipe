package io.github.eschizoid.kpipe.tracing;

import java.util.Objects;

/// The span of the record being processed, carried in a scoped value rather than a thread-local,
/// so it follows the record into `StructuredTaskScope` forks and is never confused with another
/// record's span on a reused thread. The consumer binds it around each record's processing; a
/// Kafka sink reads it to write the record's trace into the records it produces.
///
/// ```java
/// final var span = TraceContext.current();
/// span.inject(outgoing.headers());
/// ```
public final class TraceContext {

  private static final ScopedValue<Tracer.SpanScope> CURRENT = ScopedValue.newInstance();

  private TraceContext() {}

  /// @return the span of the record being processed on this thread, or [Tracer.SpanScope#noop()]
  ///     when none is
  public static Tracer.SpanScope current() {
    return CURRENT.orElse(Tracer.SpanScope.noop());
  }

  /// Runs `op` with `span` as the current record's span.
  ///
  /// @param span the record's span
  /// @param op   the work to run
  /// @param <R>  the result type
  /// @param <X>  the exception `op` may throw
  /// @return what `op` returns
  /// @throws X when `op` throws
  public static <R, X extends Throwable> R callWith(final Tracer.SpanScope span, final ScopedValue.CallableOp<R, X> op)
    throws X {
    Objects.requireNonNull(span, "span cannot be null");
    return ScopedValue.where(CURRENT, span).call(op);
  }
}
