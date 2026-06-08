package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/// Pins the default-method behavior on the [Handle] interface. Anything a downstream user
/// inherits without overriding lives here — silent behavior changes to these defaults are how
/// you break callers outside this repo.
class HandleDefaultsTest {

  /// Minimal Handle implementing only the abstract methods. The two defaults
  /// ([Handle#topKeyQueueDepths] and [Handle#close]) deliberately fall through to their
  /// interface implementations so this test exercises them.
  private static final class MinimalHandle implements Handle {

    final AtomicReference<Duration> shutdownTimeout = new AtomicReference<>();

    @Override
    public boolean isHealthy() {
      return true;
    }

    @Override
    public Map<String, Long> metrics() {
      return Map.of();
    }

    @Override
    public boolean awaitShutdown(final Duration timeout) {
      return true;
    }

    @Override
    public void awaitShutdown() {
      // no-op
    }

    @Override
    public boolean shutdownGracefully(final Duration timeout) {
      shutdownTimeout.set(timeout);
      return true;
    }
  }

  @Test
  void topKeyQueueDepthsRejectsZero() {
    final var handle = new MinimalHandle();
    final var thrown = assertThrows(IllegalArgumentException.class, () -> handle.topKeyQueueDepths(0));
    assertTrue(thrown.getMessage().contains("must be positive"), () -> "msg was: " + thrown.getMessage());
  }

  @Test
  void topKeyQueueDepthsRejectsNegative() {
    assertThrows(IllegalArgumentException.class, () -> new MinimalHandle().topKeyQueueDepths(-1));
  }

  @Test
  void topKeyQueueDepthsDefaultReturnsEmptyForValidN() {
    // The default exists so implementations outside this repo don't break when the method is
    // added — they get an empty list, NOT a NullPointerException or UnsupportedOperationException.
    final var result = new MinimalHandle().topKeyQueueDepths(5);
    assertNotNull(result);
    assertTrue(result.isEmpty());
    // List.of() returns an immutable empty list; pin that so a future refactor can't substitute
    // a mutable empty list and silently let callers mutate the "no key queues" return value.
    assertThrows(UnsupportedOperationException.class, () -> result.add(Map.entry("k", 1)));
    // Same type as List.of() — empty immutable list.
    assertEquals(List.of(), result);
  }

  @Test
  void closeDefaultCallsShutdownGracefullyWithFiveSeconds() {
    // The five-second default IS the contract — every try-with-resources caller relies on it.
    // If anyone tweaks it, this test forces them to update the Javadoc too.
    final var handle = new MinimalHandle();
    handle.close();
    assertEquals(Duration.ofSeconds(5), handle.shutdownTimeout.get());
  }
}
