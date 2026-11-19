package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/// The rebalance listener the consumer subscribes with: it runs the configured listener first, then
/// pauses newly assigned partitions while a pause source holds a pause.
class PausingOnAssignTest {

  private static final TopicPartition PARTITION = new TopicPartition("orders", 0);

  @Test
  void aPartitionAssignedWhilePausedIsPaused() {
    final var kafka = assigned();

    KPipeConsumer.pausingOnAssign(null, () -> true, kafka).onPartitionsAssigned(List.of(PARTITION));

    assertEquals(Set.of(PARTITION), kafka.paused());
  }

  @Test
  void aPartitionAssignedWhileRunningIsLeftFetching() {
    final var kafka = assigned();

    KPipeConsumer.pausingOnAssign(null, () -> false, kafka).onPartitionsAssigned(List.of(PARTITION));

    assertTrue(kafka.paused().isEmpty());
  }

  @Test
  void withNoConfiguredListenerRevokeAndLossDoNothing() {
    final var listener = KPipeConsumer.pausingOnAssign(null, () -> true, assigned());

    assertDoesNotThrow(() -> listener.onPartitionsRevoked(List.of(PARTITION)));
    assertDoesNotThrow(() -> listener.onPartitionsLost(List.of(PARTITION)));
  }

  @Test
  void theConfiguredListenerHearsEveryCallbackBeforeThePause() {
    final var kafka = assigned();
    final var heard = new ArrayList<String>();
    final var delegate = new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        heard.add("revoked");
      }

      @Override
      public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        heard.add("assigned, paused=" + !kafka.paused().isEmpty());
      }

      @Override
      public void onPartitionsLost(final Collection<TopicPartition> partitions) {
        heard.add("lost");
      }
    };
    final var listener = KPipeConsumer.pausingOnAssign(delegate, () -> true, kafka);

    listener.onPartitionsRevoked(List.of(PARTITION));
    listener.onPartitionsAssigned(List.of(PARTITION));
    listener.onPartitionsLost(List.of(PARTITION));

    assertEquals(List.of("revoked", "assigned, paused=false", "lost"), heard);
    assertEquals(Set.of(PARTITION), kafka.paused());
  }

  private static MockConsumer<byte[], byte[]> assigned() {
    final var kafka = new MockConsumer<byte[], byte[]>("earliest");
    kafka.assign(List.of(PARTITION));
    return kafka;
  }
}
