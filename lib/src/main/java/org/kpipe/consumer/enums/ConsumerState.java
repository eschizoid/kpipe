package org.kpipe.consumer.enums;

/// Represents the operational states of the consumer.
///
/// The consumer transitions through these states during its lifecycle:
///
/// * `CREATED` - Initial state after creation, before starting
/// * `RUNNING` - Actively consuming messages from the topic
/// * `PAUSED` - Consumption temporarily suspended
/// * `CLOSING` - In the process of shutting down
/// * `CLOSED` - Terminated with all resources released
public enum ConsumerState {
  /** Consumer has been created but not started. */
  CREATED,

  /** Consumer is actively processing messages. */
  RUNNING,

  /** Consumer is temporarily paused. */
  PAUSED,

  /** Consumer is in the process of shutting down. */
  CLOSING,

  /** Consumer has completed shutdown and can't be restarted. */
  CLOSED,
}
