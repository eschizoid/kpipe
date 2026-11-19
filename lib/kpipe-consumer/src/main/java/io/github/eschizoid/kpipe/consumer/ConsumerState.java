package io.github.eschizoid.kpipe.consumer;

/// The lifecycle of a consumer: `CREATED` → `RUNNING` → `CLOSING` → `CLOSED`, or `CREATED` →
/// `CLOSED` for one closed before it started. Whether a running consumer is paused is not a
/// lifecycle state; the pause sources answer that.
enum ConsumerState {
  /// Built but not started.
  CREATED,

  /// Started; its poll loop is live, paused or not.
  RUNNING,

  /// Shutting down: the poll loop has stopped or is stopping, and resources are being released.
  CLOSING,

  /// Shut down for good; it cannot be restarted.
  CLOSED,
}
