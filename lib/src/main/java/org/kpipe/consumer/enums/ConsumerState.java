package org.kpipe.consumer.enums;

/**
 * Represents the operational states of the consumer.
 *
 * <p>The consumer transitions through these states during its lifecycle:
 *
 * <ul>
 *   <li>{@code CREATED} - Initial state after creation, before starting
 *   <li>{@code RUNNING} - Actively consuming messages from the topic
 *   <li>{@code PAUSED} - Consumption temporarily suspended
 *   <li>{@code CLOSING} - In the process of shutting down
 *   <li>{@code CLOSED} - Terminated with all resources released
 * </ul>
 */
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
