package io.github.eschizoid.kpipe.consumer;

import io.github.eschizoid.kpipe.metrics.ConsumerMetrics;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;

/// Wraps a user-supplied [ConsumerMetrics] so a throwing implementation can never crash the
/// consumer. The rule that every user callback is guarded (tracer, error handler) previously had
/// one unguarded exception: the ~dozen `otelMetrics.*` call sites, where a throwing metrics
/// implementation could crash a worker mid-error-handling or abort a batch-flush callback loop.
/// Guarding once here — at construction — protects every call site at once instead of try/catching
/// each of them.
///
/// Failures are logged at WARNING with the failing method name and then swallowed; metrics are
/// observability, never worth a record.
final class GuardedConsumerMetrics implements ConsumerMetrics {

  private static final Logger LOGGER = System.getLogger(GuardedConsumerMetrics.class.getName());

  private final ConsumerMetrics delegate;

  private GuardedConsumerMetrics(final ConsumerMetrics delegate) {
    this.delegate = delegate;
  }

  /// Wraps `delegate`, or returns the no-op unchanged (nothing to guard, no extra indirection).
  ///
  /// @param delegate the user-supplied metrics, or null for none
  /// @return a guard around `delegate`, or [ConsumerMetrics#noop] when null/noop
  static ConsumerMetrics guard(final ConsumerMetrics delegate) {
    if (delegate == null || delegate == ConsumerMetrics.noop()) return ConsumerMetrics.noop();
    return new GuardedConsumerMetrics(delegate);
  }

  @Override
  public void recordMessageReceived() {
    try {
      delegate.recordMessageReceived();
    } catch (final Exception e) {
      warn("recordMessageReceived", e);
    }
  }

  @Override
  public void recordMessageProcessed() {
    try {
      delegate.recordMessageProcessed();
    } catch (final Exception e) {
      warn("recordMessageProcessed", e);
    }
  }

  @Override
  public void recordProcessingError() {
    try {
      delegate.recordProcessingError();
    } catch (final Exception e) {
      warn("recordProcessingError", e);
    }
  }

  @Override
  public void recordProcessingDuration(final long millis) {
    try {
      delegate.recordProcessingDuration(millis);
    } catch (final Exception e) {
      warn("recordProcessingDuration", e);
    }
  }

  @Override
  public void recordBackpressurePause() {
    try {
      delegate.recordBackpressurePause();
    } catch (final Exception e) {
      warn("recordBackpressurePause", e);
    }
  }

  @Override
  public void recordBackpressureTime(final long millis) {
    try {
      delegate.recordBackpressureTime(millis);
    } catch (final Exception e) {
      warn("recordBackpressureTime", e);
    }
  }

  @Override
  public void recordMessageReceived(final String topic) {
    try {
      delegate.recordMessageReceived(topic);
    } catch (final Exception e) {
      warn("recordMessageReceived(topic)", e);
    }
  }

  @Override
  public void recordMessageProcessed(final String topic) {
    try {
      delegate.recordMessageProcessed(topic);
    } catch (final Exception e) {
      warn("recordMessageProcessed(topic)", e);
    }
  }

  @Override
  public void recordProcessingError(final String topic) {
    try {
      delegate.recordProcessingError(topic);
    } catch (final Exception e) {
      warn("recordProcessingError(topic)", e);
    }
  }

  @Override
  public void recordProcessingDuration(final String topic, final long millis) {
    try {
      delegate.recordProcessingDuration(topic, millis);
    } catch (final Exception e) {
      warn("recordProcessingDuration(topic)", e);
    }
  }

  @Override
  public void recordCircuitBreakerTrip() {
    try {
      delegate.recordCircuitBreakerTrip();
    } catch (final Exception e) {
      warn("recordCircuitBreakerTrip", e);
    }
  }

  @Override
  public void recordCircuitBreakerStateChange(final Enum<?> state) {
    try {
      delegate.recordCircuitBreakerStateChange(state);
    } catch (final Exception e) {
      warn("recordCircuitBreakerStateChange", e);
    }
  }

  @Override
  public void recordCircuitBreakerTimeOpen(final long millis) {
    try {
      delegate.recordCircuitBreakerTimeOpen(millis);
    } catch (final Exception e) {
      warn("recordCircuitBreakerTimeOpen", e);
    }
  }

  private static void warn(final String method, final Exception e) {
    LOGGER.log(Level.WARNING, "ConsumerMetrics." + method + " threw; swallowing (metrics must never crash records)", e);
  }
}
