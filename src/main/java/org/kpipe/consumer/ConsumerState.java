package org.kpipe.consumer;

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
  CREATED,
  RUNNING,
  PAUSED,
  CLOSING,
  CLOSED,
}
