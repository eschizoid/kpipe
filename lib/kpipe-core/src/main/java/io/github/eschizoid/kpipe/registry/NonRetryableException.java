package io.github.eschizoid.kpipe.registry;

import java.util.Collections;
import java.util.IdentityHashMap;

/// A failure that processing the same record again cannot fix, such as a missing required field or
/// malformed bytes. Throw it from an operator or a sink to fail the record without retrying it.
///
/// KPipe retries other failures up to the configured `maxRetries`. A record that fails with this
/// exception, or with an exception caused by it, goes straight to the error handler and the
/// dead-letter topic when one is configured, and does not count as a failure toward the circuit
/// breaker: it says the record is bad, not that a downstream system is down.
public class NonRetryableException extends RuntimeException {

  /// @param message what is wrong with the record
  public NonRetryableException(final String message) {
    super(message);
  }

  /// @param message what is wrong with the record
  /// @param cause the underlying failure
  public NonRetryableException(final String message, final Throwable cause) {
    super(message, cause);
  }

  /// @param failure a failure, possibly wrapping others
  /// @return whether `failure` or any exception in its cause chain is a [NonRetryableException]
  public static boolean isNonRetryable(final Throwable failure) {
    final var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
    for (var current = failure; current != null && seen.add(current); current = current.getCause()) {
      if (current instanceof NonRetryableException) return true;
    }
    return false;
  }
}
