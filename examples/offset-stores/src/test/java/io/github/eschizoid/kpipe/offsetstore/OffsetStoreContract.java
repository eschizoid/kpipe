package io.github.eschizoid.kpipe.offsetstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.eschizoid.kpipe.consumer.OffsetStore;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/// What every [OffsetStore] in this module does the same way. Each store's test class extends this
/// and says only how to build that store.
abstract class OffsetStoreContract {

  private final String topic = "orders-" + UUID.randomUUID();
  private final TopicPartition p0 = new TopicPartition(topic, 0);
  private final TopicPartition p1 = new TopicPartition(topic, 1);
  private final TopicPartition p2 = new TopicPartition(topic, 2);

  /// @param group the consumer group the returned store keeps offsets for
  /// @return a store over a table that exists and may hold other groups' or topics' offsets
  abstract OffsetStore store(String group);

  @Test
  void aPartitionNeverCommittedIsLeftOut() {
    assertEquals(Map.of(), store("payments").load(List.of(p0, p1)));
  }

  @Test
  void loadReturnsWhatCommitWrote() {
    final var store = store("payments");
    store.commit(Map.of(p0, 10L, p1, 20L, p2, 30L));

    assertEquals(Map.of(p0, 10L, p1, 20L, p2, 30L), store.load(List.of(p0, p1, p2)));
  }

  @Test
  void aLaterCommitReplacesTheOffsetEvenWithALowerOne() {
    final var store = store("payments");
    store.commit(Map.of(p0, 10L));
    store.commit(Map.of(p0, 4L));

    assertEquals(Map.of(p0, 4L), store.load(List.of(p0)), "the last write wins");
  }

  @Test
  void aCommitLeavesPartitionsItDoesNotNameAlone() {
    final var store = store("payments");
    store.commit(Map.of(p0, 10L, p1, 20L));
    store.commit(Map.of(p1, 21L));

    assertEquals(Map.of(p0, 10L, p1, 21L), store.load(List.of(p0, p1)));
  }

  @Test
  void loadReturnsOnlyTheRequestedPartitions() {
    final var store = store("payments");
    store.commit(Map.of(p0, 10L, p1, 20L));

    assertEquals(Map.of(p1, 20L), store.load(List.of(p1, p2)));
  }

  @Test
  void groupsDoNotSeeEachOthersOffsets() {
    store("payments").commit(Map.of(p0, 10L));
    store("refunds").commit(Map.of(p0, 99L));

    assertEquals(Map.of(p0, 10L), store("payments").load(List.of(p0)));
    assertEquals(Map.of(p0, 99L), store("refunds").load(List.of(p0)));
  }
}
