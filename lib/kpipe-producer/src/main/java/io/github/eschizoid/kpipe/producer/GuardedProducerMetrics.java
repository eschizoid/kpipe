package io.github.eschizoid.kpipe.producer;

import io.github.eschizoid.kpipe.metrics.ProducerMetrics;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;

/// Wraps a user-supplied [ProducerMetrics] so a throwing implementation can never corrupt send
/// semantics. Unguarded, a metrics implementation that threw on `recordMessageSent()` would turn a
/// SUCCESSFUL send into a thrown "send failed" (the counter call sits after the broker ack), and a
/// throwing `recordDlqSent()` would make a successful DLQ park report as a DLQ failure — a user
/// observability callback must never change data-path outcomes.
///
/// Failures are logged at WARNING and swallowed; metrics are observability, never worth a record.
final class GuardedProducerMetrics implements ProducerMetrics {

  private static final Logger LOGGER = System.getLogger(GuardedProducerMetrics.class.getName());

  private final ProducerMetrics delegate;

  private GuardedProducerMetrics(final ProducerMetrics delegate) {
    this.delegate = delegate;
  }

  /// Wraps `delegate`, or returns the no-op unchanged (nothing to guard, no extra indirection).
  ///
  /// @param delegate the user-supplied metrics, or null for none
  /// @return a guard around `delegate`, or [ProducerMetrics#noop] when null/noop
  static ProducerMetrics guard(final ProducerMetrics delegate) {
    if (delegate == null || delegate == ProducerMetrics.noop()) return ProducerMetrics.noop();
    return new GuardedProducerMetrics(delegate);
  }

  @Override
  public void recordMessageSent() {
    try {
      delegate.recordMessageSent();
    } catch (final Exception e) {
      warn("recordMessageSent", e);
    }
  }

  @Override
  public void recordMessageFailed() {
    try {
      delegate.recordMessageFailed();
    } catch (final Exception e) {
      warn("recordMessageFailed", e);
    }
  }

  @Override
  public void recordDlqSent() {
    try {
      delegate.recordDlqSent();
    } catch (final Exception e) {
      warn("recordDlqSent", e);
    }
  }

  @Override
  public void recordDlqFailed() {
    try {
      delegate.recordDlqFailed();
    } catch (final Exception e) {
      warn("recordDlqFailed", e);
    }
  }

  private static void warn(final String method, final Exception e) {
    LOGGER.log(Level.WARNING, "ProducerMetrics." + method + " threw; swallowing (metrics must never affect sends)", e);
  }
}
