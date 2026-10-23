package io.github.eschizoid.kpipe.consumer;

import java.nio.channels.ClosedByInterruptException;

/// Recognises an exception caused by an interrupt, such as the one shutdown delivers to a record
/// still running. An interrupted record has not failed: it never finished, so its offset must stay
/// unmarked and the record redelivered. Treating it as a failure would, with no DLQ configured,
/// log it and commit past a record that never reached its destination.
final class Interruptions {

  private Interruptions() {}

  /// Whether `error` or anything in its cause chain is an interruption.
  ///
  /// @param error the exception to inspect
  /// @return true if an [InterruptedException] or [ClosedByInterruptException] is in the chain
  static boolean isInterruptionRelated(final Throwable error) {
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof InterruptedException || current instanceof ClosedByInterruptException) return true;
    }
    return false;
  }
}
