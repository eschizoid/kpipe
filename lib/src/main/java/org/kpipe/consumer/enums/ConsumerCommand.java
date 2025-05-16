package org.kpipe.consumer.enums;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

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
  /** Pause the consumer from processing messages. */
  PAUSE,

  /** Resume a paused consumer. */
  RESUME,

  /** Close the consumer and release resources. */
  CLOSE,

  /** Track an offset in the OffsetManager. */
  TRACK_OFFSET,

  /** Mark an offset as processed in the OffsetManager. */
  MARK_OFFSET_PROCESSED,

  /** Commit offsets to Kafka. */
  COMMIT_OFFSETS;

  /** The record associated with the command, if applicable. */
  private ConsumerRecord<?, ?> record;

  private Map<TopicPartition, OffsetAndMetadata> offsets;
  private String commitId;

  /**
   * Sets the record associated with this command.
   *
   * @param record the record to associate with this command
   * @return this ConsumerCommand instance
   */
  public ConsumerCommand withRecord(final ConsumerRecord<?, ?> record) {
    this.record = record;
    return this;
  }

  /**
   * Gets the record associated with this command.
   *
   * @return the record associated with this command, or null if not set
   */
  public ConsumerRecord<?, ?> getRecord() {
    return record;
  }

  /**
   * Associates commit offsets with this command.
   *
   * @param offsets the offsets to commit
   * @return this command instance for method chaining
   */
  public ConsumerCommand withOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
    this.offsets = offsets;
    return this;
  }

  /**
   * Gets the offsets associated with this command.
   *
   * @return the offsets map, or null if not set
   */
  public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
    return offsets;
  }

  /**
   * Sets the commit ID associated with this command.
   *
   * @param commitId the commit ID to associate with this command
   * @return this ConsumerCommand instance
   */
  public ConsumerCommand withCommitId(final String commitId) {
    this.commitId = commitId;
    return this;
  }

  /**
   * Gets the commit ID associated with this command.
   *
   * @return the commit ID, or null if not set
   */
  public String getCommitId() {
    return commitId;
  }
}
