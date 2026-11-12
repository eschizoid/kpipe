package io.github.eschizoid.kpipe.test;

import io.github.eschizoid.kpipe.record.RecordContext;
import io.github.eschizoid.kpipe.sink.BatchResult;
import io.github.eschizoid.kpipe.sink.BatchSink;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

/// A [MessageSink] that records every value it receives, for assertions in tests, together with
/// the record each value came from as the sink saw it: after any `rekey` or `withHeader`.
///
/// Thread-safe: values may arrive concurrently from parallel virtual-thread workers.
/// [#captured] and [#records] return immutable snapshots, so the assertion never observes a
/// half-appended list. There is deliberately no assertion DSL here — use `assertEquals`, AssertJ,
/// or Hamcrest on the returned list.
///
/// ```java
/// final var captured = new CapturingSink<Map<String, Object>>();
/// // ... drive records through a TestStream with .toCustom(captured) ...
/// assertEquals(List.of(expected), captured.captured());
/// assertEquals("order-1", captured.records().getFirst().keyString());
/// ```
///
/// For a batch route, pass [#asBatchSink()] to `toBatch`; it captures each value with its record
/// from `RecordContext.currentBatch()`.
///
/// @param <T> the pipeline value type
public final class CapturingSink<T> implements MessageSink<T> {

  private final CopyOnWriteArrayList<Captured<T>> records = new CopyOnWriteArrayList<>();

  /// One delivered value and the record it came from. Compare its `value()` and `keyString()` in
  /// assertions rather than whole `Captured` instances, since a context compares by identity.
  ///
  /// @param value   the delivered value
  /// @param context the record as the sink saw it; null when no record was current: outside a
  ///                pipeline, on another thread, or when a batch sink calls [#accept] (use
  ///                [#asBatchSink()] there)
  /// @param <T>     the pipeline value type
  public record Captured<T>(T value, RecordContext context) {
    /// @return the record's key decoded as UTF-8; null when it has none or no record was current
    public String keyString() {
      return context == null ? null : context.keyString();
    }

    /// @param name the header name
    /// @return the header's last value decoded as UTF-8, or empty when absent or no record was
    /// current
    public Optional<String> headerString(final String name) {
      return context == null ? Optional.empty() : context.headerString(name);
    }
  }

  /// Records the delivered value with the current record.
  ///
  /// @param value the value delivered by the pipeline
  @Override
  public void accept(final T value) {
    records.add(new Captured<>(value, RecordContext.isBound() ? RecordContext.current() : null));
  }

  /// A batch sink that captures each flushed value with its record from
  /// `RecordContext.currentBatch()` and reports the whole batch as succeeded.
  ///
  /// @return the batch sink
  public BatchSink<T> asBatchSink() {
    return values -> {
      final var contexts = RecordContext.currentBatch();
      for (int i = 0; i < values.size(); i++) records.add(new Captured<>(values.get(i), contexts.get(i)));
      return BatchResult.allSucceeded(values.size());
    };
  }

  /// Returns an immutable snapshot of every value captured so far, in delivery order.
  ///
  /// @return the captured values
  public List<T> captured() {
    return records.stream().map(Captured::value).toList();
  }

  /// Returns an immutable snapshot of every value captured so far with its record, in delivery
  /// order.
  ///
  /// @return the captured values and records
  public List<Captured<T>> records() {
    return List.copyOf(records);
  }

  /// Returns the number of values captured so far.
  ///
  /// @return the capture count
  public int count() {
    return records.size();
  }

  /// Discards all captured values, so one sink instance can be reused across test phases.
  public void clear() {
    records.clear();
  }
}
