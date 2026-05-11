package org.kpipe.consumer;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/// Tracks which sources are currently holding the consumer paused. Replaces three coordinated
/// AtomicBoolean flags (manual / backpressure / circuit breaker) with one bitmask + arbitration
/// methods so the call sites don't have to AND-together multiple checks.
///
/// **Why a coordinator.** Three independent features can each ask the consumer to pause:
/// the user (manual), backpressure (in-flight or lag watermark crossed), and the circuit
/// breaker (failure rate threshold crossed). Before this class, each owned its own
/// AtomicBoolean and every resume site had to check the other two — which already caused one
/// real bug (backpressure auto-resuming over a CB-held pause). With the coordinator, the
/// rule is single: the consumer is paused iff at least one source holds it; resume only fires
/// when the **last** source releases.
///
/// **Bitmask, not EnumSet, for hot-path reads.** [#isPaused] runs on every consumer-loop
/// iteration and inside `processRecord`'s finally block. A single `mask.get() != 0` is
/// allocation-free and lock-free; an `EnumSet` would cost an indirection per read. The
/// `EnumSet` is only materialised by [#currentSources] for logging.
///
/// **Concurrency.** Both `requestPause` and `releasePause` use `getAndUpdate` so concurrent
/// modifications compose: if two sources request simultaneously they both end up in the mask;
/// only the first caller observes the `was-empty → not-empty` transition. Same for release.
final class PauseCoordinator {

  /// Identifies which feature is asking the consumer to pause.
  public enum Source {
    /// Set by `KPipeConsumer.pause()` / cleared by `KPipeConsumer.resume()`.
    MANUAL(1),
    /// Set/cleared by `checkBackpressure` when the configured strategy's metric crosses
    /// the high / low watermarks.
    BACKPRESSURE(2),
    /// Set by the circuit breaker on trip; cleared by the half-open probe.
    CIRCUIT_BREAKER(4);

    final int bit;

    Source(final int bit) {
      this.bit = bit;
    }
  }

  private final AtomicInteger mask = new AtomicInteger(0);

  /// Adds `source` to the held set.
  ///
  /// @return `true` iff this call caused the transition from "no sources holding" to "at least
  ///     one source holding" — the caller should now invoke `internalPause()`. Idempotent:
  ///     calling twice with the same source returns `true` only the first time.
  boolean requestPause(final Source source) {
    return mask.getAndUpdate(m -> m | source.bit) == 0;
  }

  /// Removes `source` from the held set.
  ///
  /// @return `true` iff this call caused the transition from "at least one source holding" to
  ///     "no sources holding" — the caller should now invoke `internalResume()`. Idempotent.
  boolean releasePause(final Source source) {
    final var prev = mask.getAndUpdate(m -> m & ~source.bit);
    return prev != 0 && (prev & ~source.bit) == 0;
  }

  /// Returns whether `source` is currently in the held set.
  boolean isHeldBy(final Source source) {
    return (mask.get() & source.bit) != 0;
  }

  /// Returns whether ANY source is holding the consumer paused.
  boolean isPaused() {
    return mask.get() != 0;
  }

  /// Snapshot of which sources currently hold the pause. Allocates on each call — use only for
  /// logging.
  Set<Source> currentSources() {
    final var m = mask.get();
    final var set = EnumSet.noneOf(Source.class);
    for (final var s : Source.values()) if ((m & s.bit) != 0) set.add(s);
    return set;
  }
}
