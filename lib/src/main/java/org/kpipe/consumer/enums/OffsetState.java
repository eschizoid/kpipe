package org.kpipe.consumer.enums;

/**
 * Represents the possible operational states of an OffsetManager.
 *
 * <p>The state transitions generally follow this sequence:
 *
 * <ul>
 *   <li>CREATED - Initial state upon instantiation
 *   <li>RUNNING - Active state when managing offsets
 *   <li>STOPPING - Transitional state during shutdown
 *   <li>STOPPED - Final state after shutdown completion
 * </ul>
 *
 * <p>State transitions are atomic and thread-safe when managed properly.
 */
public enum OffsetState {
  /**
   * Initial state when the OffsetManager is created but not yet started. No offset tracking or
   * committing occurs in this state.
   */
  CREATED,

  /**
   * Active state when the OffsetManager is tracking and committing offsets. Periodic commit tasks
   * are scheduled in this state.
   */
  RUNNING,

  /**
   * Transitional state during shutdown process. Final commits may be performed, but no new offsets
   * are accepted.
   */
  STOPPING,

  /**
   * Final state after the OffsetManager has been fully stopped. All resources have been released
   * and no operations can be performed.
   */
  STOPPED,
}
