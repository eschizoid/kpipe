package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.junit.jupiter.api.Test;

/// A periodic commit that fires during teardown waits on a command queue nobody serves anymore.
/// Stopping the offset manager must not wait for it past the scheduler's stop budget: it gives up
/// after [ConsumerDefaults#COMMIT_SCHEDULER_STOP] and cancels the stuck commit.
class CommitSchedulerStopTest {

  @Test
  void stopGivesUpOnAPeriodicCommitStuckOnAnUnservedQueue() throws InterruptedException {
    final var commitRequested = new CountDownLatch(1);
    // Like a command queue the consumer thread no longer drains: requests are taken, never served.
    final CommitExecutor unserved = _ -> {
      commitRequested.countDown();
      return new java.util.concurrent.CompletableFuture<>();
    };
    final var manager = KafkaOffsetManager.builder(new MockConsumer<byte[], byte[]>("earliest"))
      .withCommitExecutor(unserved)
      .withCommitInterval(Duration.ofMillis(50))
      .build();
    manager.start();
    final var record = new ConsumerRecord<>("topic", 0, 0L, "k".getBytes(UTF_8), "v".getBytes(UTF_8));
    manager.trackOffset(record);
    manager.markOffsetProcessed(record);
    assertTrue(commitRequested.await(5, TimeUnit.SECONDS), "a periodic commit should be requested");

    final long start = System.nanoTime();
    manager.stop();
    final var elapsed = Duration.ofNanos(System.nanoTime() - start);

    final var budget = ConsumerDefaults.COMMIT_SCHEDULER_STOP;
    assertTrue(elapsed.compareTo(budget.plusSeconds(2)) < 0, "stop() must give up within its budget; took " + elapsed);
    assertTrue(
      elapsed.compareTo(budget.minusMillis(500)) > 0,
      "stop() returned before the budget, so the stuck commit was never waited on; took " + elapsed
    );
  }
}
