package io.github.eschizoid.kpipe.test;

import java.util.List;

/// The outcome of a [CrashRestartHarness] run: what each consumer instance delivered to its sink,
/// and the offset that was durably committed before the crash.
///
/// The components are the raw material for at-least-once assertions; there is deliberately
/// no assertion DSL — use `assertEquals`, AssertJ, or Hamcrest on the returned lists.
/// `firstRun` is what consumer A delivered before it crashed (the prefix `[0, P)`);
/// `secondRun` is what the restarted consumer B delivered when it resumed from the
/// committed offset (`[k, N)`). Both are post-pipeline sink outputs.
///
/// The key at-least-once property to assert is that `secondRun` re-delivers every value in
/// the uncommitted tail — [#redeliveredTail] is that tail (the offsets `[k, P)` as the sink
/// delivers them: operators applied, filtered records dropped), independent of capture
/// order, so it is meaningful under every processing mode:
///
/// ```java
/// assertTrue(result.secondRun().containsAll(result.redeliveredTail()));
/// ```
///
/// @param firstRun the values consumer A delivered before crashing (immutable)
/// @param secondRun the values the restarted consumer B delivered (immutable)
/// @param committedOffset the offset A durably committed before the crash (`k`)
/// @param redeliveredTail the sink-delivered values for offsets `[k, P)` — A processed but
///     never committed them, so a correct at-least-once pipeline must re-deliver them on
///     restart (immutable)
/// @param <T> the pipeline value type
public record CrashRestartResult<T>(
  List<T> firstRun,
  List<T> secondRun,
  long committedOffset,
  List<T> redeliveredTail
) {
  /// Canonical constructor — defensively copies every list so the result is immutable.
  public CrashRestartResult {
    firstRun = List.copyOf(firstRun);
    secondRun = List.copyOf(secondRun);
    redeliveredTail = List.copyOf(redeliveredTail);
  }
}
