package io.github.eschizoid.kpipe.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class CapturingSinkTest {

  @Test
  void capturesInDeliveryOrder() {
    final var sink = new CapturingSink<String>();
    sink.accept("a");
    sink.accept("b");
    sink.accept("c");

    assertEquals(List.of("a", "b", "c"), sink.captured());
    assertEquals(3, sink.count());
  }

  @Test
  void capturedReturnsAnImmutableSnapshot() {
    final var sink = new CapturingSink<String>();
    sink.accept("a");
    final var snapshot = sink.captured();
    sink.accept("b");

    assertEquals(List.of("a"), snapshot, "a snapshot must not see later deliveries");
    assertThrows(UnsupportedOperationException.class, () -> snapshot.add("x"));
  }

  @Test
  void clearResetsTheSink() {
    final var sink = new CapturingSink<String>();
    sink.accept("a");
    sink.clear();

    assertEquals(List.of(), sink.captured());
    assertEquals(0, sink.count());
  }

  @Test
  void concurrentDeliveriesAreAllCaptured() throws InterruptedException {
    // Real virtual threads, not CompletableFuture — mirrors how parallel-mode workers deliver.
    final var sink = new CapturingSink<Integer>();
    final var threads = 50;
    final var start = new CountDownLatch(1);
    final var done = new CountDownLatch(threads);
    for (int i = 0; i < threads; i++) {
      final var value = i;
      Thread.ofVirtual().start(() -> {
        try {
          start.await();
          sink.accept(value);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
    }
    start.countDown();
    assertTrue(done.await(5, TimeUnit.SECONDS), "virtual threads must finish");

    final var seen = new HashSet<>(sink.captured());
    assertEquals(threads, sink.count(), "no delivery may be lost under concurrency");
    for (int i = 0; i < threads; i++) assertTrue(seen.contains(i));
  }
}
