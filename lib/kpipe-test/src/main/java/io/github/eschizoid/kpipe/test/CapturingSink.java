package io.github.eschizoid.kpipe.test;

import io.github.eschizoid.kpipe.sink.MessageSink;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/// A [MessageSink] that records every value it receives, for assertions in tests.
///
/// Thread-safe: values may arrive concurrently from parallel virtual-thread workers.
/// [#captured] returns an immutable snapshot, so the assertion never observes a half-appended
/// list. There is deliberately no assertion DSL here — use `assertEquals`, AssertJ, or Hamcrest
/// on the returned list.
///
/// ```java
/// final var captured = new CapturingSink<Map<String, Object>>();
/// // ... drive records through a TestStream with .toCustom(captured) ...
/// assertEquals(List.of(expected), captured.captured());
/// ```
///
/// @param <T> the pipeline value type
public final class CapturingSink<T> implements MessageSink<T> {

  private final CopyOnWriteArrayList<T> values = new CopyOnWriteArrayList<>();

  /// Records the delivered value.
  ///
  /// @param value the value delivered by the pipeline
  @Override
  public void accept(final T value) {
    values.add(value);
  }

  /// Returns an immutable snapshot of every value captured so far, in delivery order.
  ///
  /// @return the captured values
  public List<T> captured() {
    return List.copyOf(values);
  }

  /// Returns the number of values captured so far.
  ///
  /// @return the capture count
  public int count() {
    return values.size();
  }

  /// Discards all captured values, so one sink instance can be reused across test phases.
  public void clear() {
    values.clear();
  }
}
