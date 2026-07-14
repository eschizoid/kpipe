package io.github.eschizoid.kpipe.test;

import java.util.List;

/// The outcome of a [CrashRestartHarness] run: what each consumer instance delivered to
/// its sink, and the offset that was durably committed before the crash.
///
/// The components are the raw material for at-least-once assertions; there is deliberately
/// no assertion DSL — use `assertEquals`, AssertJ, or Hamcrest on the returned lists.
/// `firstRun` is what consumer A delivered before it crashed (the prefix `[0, P)`).
/// `secondRun` is what the restarted consumer B delivered over the resume window
/// `[k, N)` — the log a broker would replay from committed offset `k`. Both are
/// post-pipeline sink outputs.
///
/// [#uncommittedTail] is the offsets `[k, P)` A processed but never committed, in the shape
/// the sink delivers them (operators applied, filtered records dropped) — a correct
/// at-least-once pipeline re-processes them on restart. It is defined by offset, not capture
/// order, so it is meaningful under every processing mode. B's resume window includes it:
///
/// ```java
/// assertTrue(result.secondRun().containsAll(result.uncommittedTail()));
/// ```
///
/// @param firstRun the values consumer A delivered before crashing (immutable)
/// @param secondRun the values the restarted consumer B delivered over `[k, N)` (immutable)
/// @param committedOffset the offset A durably committed before the crash (`k`)
/// @param uncommittedTail the sink-delivered values for offsets `[k, P)` — A processed but
///     never committed them, so a correct at-least-once pipeline re-processes them on
///     restart (immutable)
/// @param <T> the pipeline value type
public record CrashRestartResult<T>(
  List<T> firstRun,
  List<T> secondRun,
  long committedOffset,
  List<T> uncommittedTail
) {
  /// Canonical constructor — defensively copies every list so the result is immutable.
  public CrashRestartResult {
    firstRun = List.copyOf(firstRun);
    secondRun = List.copyOf(secondRun);
    uncommittedTail = List.copyOf(uncommittedTail);
  }
}
