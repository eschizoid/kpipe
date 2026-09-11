package io.github.eschizoid.kpipe.schemaregistry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.registry.SchemaResolver;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

/// Pins the cache's behaviour past the cardinality its no-eviction design assumes.
///
/// The warning is the whole point of the change, so it is asserted rather than merely executed:
/// exactly one line however far past the threshold the cache grows, and none below it. A
/// sustained miss stream must not log at record rate. `System.Logger` output is captured through
/// the JUL handler it bridges to, the same way `KeyOrderedDispatcherTest` pins the KEY_ORDERED
/// saturation warning.
class CachedSchemaResolverSizeWarningTest {

  /// Captures WARNING records from the resolver's logger for the duration of `body`.
  ///
  /// @param body what to run while capturing
  /// @return the warnings emitted
  private static List<LogRecord> captureWarnings(final Runnable body) {
    final var julLogger = Logger.getLogger(CachedSchemaResolver.class.getName());
    final var captured = new CopyOnWriteArrayList<LogRecord>();
    final var handler = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        if (record.getLevel().intValue() >= Level.WARNING.intValue()) captured.add(record);
      }

      @Override
      public void flush() {}

      @Override
      public void close() {}
    };
    final var originalLevel = julLogger.getLevel();
    final var originalUseParent = julLogger.getUseParentHandlers();
    julLogger.addHandler(handler);
    julLogger.setLevel(Level.ALL);
    julLogger.setUseParentHandlers(false);
    try {
      body.run();
    } finally {
      julLogger.removeHandler(handler);
      julLogger.setLevel(originalLevel);
      julLogger.setUseParentHandlers(originalUseParent);
    }
    return captured;
  }

  @Test
  void sustainedGrowthPastTheThresholdLogsExactlyOnce() {
    final var warnings = captureWarnings(() -> {
      try (final var resolver = new CachedSchemaResolver(id -> "s" + id)) {
        for (var id = 0; id < 5_000; id++) {
          resolver.lookupById(id);
        }
      }
    });

    assertEquals(
      1,
      warnings.size(),
      "sustained growth must log once, not once per record — a stuck producer would otherwise " +
        "flood the log at poll rate"
    );
    final var message = warnings.getFirst().getMessage();
    assertTrue(message.contains("never evicts"), "the warning should name the design property that was outgrown");
    assertTrue(
      message.contains("bounded cache"),
      "the warning should name the remedy; a diagnosis an operator cannot act on is half a warning"
    );
  }

  @Test
  void stayingBelowTheThresholdLogsNothing() {
    final var warnings = captureWarnings(() -> {
      try (final var resolver = new CachedSchemaResolver(id -> "s" + id)) {
        for (var id = 0; id < 500; id++) {
          resolver.lookupById(id);
        }
      }
    });

    assertEquals(0, warnings.size(), "a cache within its assumed cardinality must stay silent");
  }

  @Test
  void concurrentCrossingsStillLogOnce() throws InterruptedException {
    final var start = new CountDownLatch(1);
    final var done = new CountDownLatch(16);
    final var resolver = new CachedSchemaResolver(id -> "s" + id);

    final var warnings = captureWarnings(() -> {
      for (var t = 0; t < 16; t++) {
        final var offset = t * 1_000;
        Thread.ofPlatform()
          .daemon()
          .start(() -> {
            try {
              start.await();
              for (var i = 0; i < 1_000; i++) {
                resolver.lookupById(offset + i);
              }
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              done.countDown();
            }
          });
      }
      start.countDown();
      try {
        done.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    resolver.close();

    assertEquals(1, warnings.size(), "sixteen threads crossing the threshold together must still log once");
  }

  @Test
  void crossingTheThresholdDoesNotDisturbCaching() {
    final var loads = new AtomicInteger();
    final SchemaResolver counting = id -> {
      loads.incrementAndGet();
      return "{\"type\":\"string\",\"id\":" + id + "}";
    };

    try (final var resolver = new CachedSchemaResolver(counting)) {
      for (var id = 0; id < 1_200; id++) {
        resolver.lookupById(id);
      }
      for (var id = 0; id < 1_200; id++) {
        resolver.lookupById(id);
      }

      assertEquals(1_200, loads.get(), "each distinct id should load exactly once");
      assertEquals(1_200, resolver.size(), "every entry should be retained; this cache does not evict");
      assertEquals(1_200, resolver.hitCount(), "the second pass should be served entirely from cache");
      assertEquals(1_200, resolver.missCount(), "the first pass should account for every miss");
    }
  }

  /// Pins the threshold and the comparison exactly, rather than bracketing it loosely.
  ///
  /// The other tests only establish that 500 is silent and 5,000 warns, which leaves the constant
  /// free to drift anywhere between and the boundary untested. The javadoc states a thousand in
  /// prose, and prose does not hold a number still.
  @Test
  void theThresholdIsExactlyOneThousand() {
    final var atThreshold = captureWarnings(() -> {
      try (final var resolver = new CachedSchemaResolver(id -> "s" + id)) {
        for (var id = 0; id < 1_000; id++) {
          resolver.lookupById(id);
        }
      }
    });
    assertEquals(0, atThreshold.size(), "exactly a thousand distinct ids is within the assumption, not past it");

    final var pastThreshold = captureWarnings(() -> {
      try (final var resolver = new CachedSchemaResolver(id -> "s" + id)) {
        for (var id = 0; id < 1_001; id++) {
          resolver.lookupById(id);
        }
      }
    });
    assertEquals(1, pastThreshold.size(), "one id past the threshold must warn");
  }

  /// Pins the cardinality check to the miss path.
  ///
  /// [ConcurrentHashMap#size] sums the map's counter cells rather than reading a field, so
  /// evaluating it per lookup costs real time on a warm cache — which is the steady state this
  /// resolver exists to produce.
  ///
  /// Adding the check to the hit path while leaving the miss path untouched changes nothing else
  /// observable: the warning still fires exactly once, at the same cardinality, and every other
  /// test in this class still passes. This assertion is the only one that fails. Moving the call
  /// to the top of `lookupById` instead also trips `theThresholdIsExactlyOneThousand`, because
  /// the check then runs before the entry is inserted and the boundary shifts by one.
  @Test
  void theCardinalityCheckStaysOffTheCacheHitPath() {
    try (final var resolver = new CachedSchemaResolver(id -> "s" + id)) {
      resolver.lookupById(7);
      assertEquals(1, resolver.unboundedCheckCount(), "the miss that populated the entry evaluates the check once");

      for (var i = 0; i < 10_000; i++) {
        resolver.lookupById(7);
      }

      assertEquals(10_000, resolver.hitCount(), "every lookup after the first should be served from cache");
      assertEquals(
        1,
        resolver.unboundedCheckCount(),
        "ten thousand cache hits must not evaluate the cardinality check — it belongs behind the " +
          "hit-path early return, where a warm cache never pays for it"
      );
    }
  }
}
