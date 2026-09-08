package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// Establishes whether Fray can explore virtual threads at all, independently of anything kpipe
/// does with them.
///
/// This exists because the dispatcher scenarios report `Iterations: 0` forever and the cause was
/// not identifiable from their own output — they build a dispatcher, a pipeline and a sink, so a
/// hang there has many candidate explanations. These two tests have one moving part each. If they
/// pass, virtual threads are fine and the dispatcher scenarios are doing something else wrong. If
/// they hang, the answer is upstream and the dispatchers are simply the first place kpipe hit it.
///
/// Named to sort ahead of the dispatcher tests so its result is on record even when a later class
/// takes the job down.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class BasicVirtualThreadFrayTest {

  /// One virtual thread, started and joined. Nothing else.
  @FrayTest(iterations = 10)
  void frayCanRunAPlainVirtualThread() {
    final var ran = new AtomicBoolean();
    final var t = Thread.ofVirtual().unstarted(() -> ran.set(true));
    t.start();
    try {
      t.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted joining the virtual thread", e);
    }
    assertTrue(ran.get(), "the virtual thread body never ran");
  }

  /// The same question for the virtual-thread-per-task executor, which is what ParallelDispatcher
  /// uses. Separated from the plain-thread case because the executor also brings a shutdown path.
  @FrayTest(iterations = 10)
  void frayCanRunAVirtualThreadPerTaskExecutor() {
    final var count = new AtomicInteger();
    try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      executor.submit(count::incrementAndGet);
      executor.submit(count::incrementAndGet);
    }
    assertEquals(2, count.get(), "both executor tasks should have run");
  }
}
