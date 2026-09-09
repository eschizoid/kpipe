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

/// Pins Fray's ability to explore virtual threads, which the rest of the suite depends on and
/// which is expensive enough to shape how the other scenarios are written.
///
/// Each iteration costs roughly 30 seconds for any scenario touching a virtual thread: the JDK
/// builds `VirtualThread`'s default scheduler with a 30-second keep-alive, so its carrier threads
/// idle out on that schedule, and Fray waits for every registered thread to reach a completed
/// state. Platform-thread scenarios in this suite run 500 iterations in a few seconds. That gap
/// is why the dispatcher scenarios inject a platform-thread factory rather than exercising the
/// production virtual-thread path.
///
/// Two tests with one moving part each, so a failure here means Fray, not kpipe.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class BasicVirtualThreadFrayTest {

  /// One virtual thread, started and joined. Nothing else.
  @FrayTest(iterations = 3)
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
  @FrayTest(iterations = 3)
  void frayCanRunAVirtualThreadPerTaskExecutor() {
    final var count = new AtomicInteger();
    try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      executor.submit(count::incrementAndGet);
      executor.submit(count::incrementAndGet);
    }
    assertEquals(2, count.get(), "both executor tasks should have run");
  }
}
