package io.github.eschizoid.kpipe.pipeline;

/// A record whose bytes cannot be decoded: a null or empty value, an envelope that is too short or
/// has the wrong magic byte, or a payload the format cannot parse. The same bytes fail the same
/// way every time, so the record is not retried; it goes straight to the error handler and the
/// dead-letter topic when one is configured.
///
/// Every built-in format throws it for bad bytes, and a custom [MessageFormat] should too. A
/// failed schema-registry lookup is not one: the registry may be down, so that failure is retried.
/// An error handler can tell bad bytes from a record an operator rejected with
/// `error.exception() instanceof MalformedRecordException`.
public class MalformedRecordException extends NonRetryableException {

  /// @param message what is wrong with the bytes
  public MalformedRecordException(final String message) {
    super(message);
  }

  /// @param message what is wrong with the bytes
  /// @param cause the decoder's own failure
  public MalformedRecordException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
