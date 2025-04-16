package org.kpipe.consumer;

/**
 * Represents commands that can be sent to a consumer.
 *
 * <p>These commands control the consumer's operational behavior through its lifecycle:
 *
 * <ul>
 *   <li>{@code PAUSE} - Temporarily stops consumption from assigned partitions
 *   <li>{@code RESUME} - Restarts consumption after being paused
 *   <li>{@code CLOSE} - Terminates the consumer and releases resources
 * </ul>
 */
public enum ConsumerCommand {
  PAUSE,
  RESUME,
  CLOSE,
}
