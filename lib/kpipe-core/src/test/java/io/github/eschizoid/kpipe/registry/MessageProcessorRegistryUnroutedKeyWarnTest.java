package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

/// Verifies the unrouted-key WARNING is deduped: the load-bearing WARN that an operator/sink key
/// is unrouted must fire once per distinct key, not once per record, so a sustained misconfig
/// can't flood logs at the consumer's poll rate.
///
/// We capture System.Logger output via the JUL handler the System.Logger bridges to (same
/// approach as the KEY_ORDERED saturation test). Note: JUL's `LogRecord.getMessage()` carries the
/// raw `{0}` format string (substitution happens in the formatter), so assertions match the raw
/// message and read the key off `getParameters()`.
class MessageProcessorRegistryUnroutedKeyWarnTest {

  @Test
  void unroutedOperatorKeyWarnsOncePerDistinctKey() {
    final var julLogger = Logger.getLogger(MessageProcessorRegistry.class.getName());
    final var captured = new CopyOnWriteArrayList<LogRecord>();
    final var handler = capturingHandler(captured);
    final var originalLevel = julLogger.getLevel();
    final var originalUseParent = julLogger.getUseParentHandlers();
    julLogger.addHandler(handler);
    julLogger.setLevel(Level.ALL);
    julLogger.setUseParentHandlers(false);

    try {
      final var registry = new MessageProcessorRegistry();
      final var keyA = RegistryKey.of("missingOpA", Object.class);
      final var keyB = RegistryKey.of("missingOpB", Object.class);

      // Same key looked up many times (per-record simulation) → exactly one WARN.
      final var opA = registry.getOperator(keyA);
      for (int i = 0; i < 50; i++) opA.apply("v" + i);
      // A second distinct key → one more WARN.
      final var opB = registry.getOperator(keyB);
      for (int i = 0; i < 50; i++) opB.apply("v" + i);

      final var opWarns = captured
        .stream()
        .filter(r -> r.getMessage() != null && r.getMessage().contains("No operator registered under key"))
        .toList();
      assertEquals(
        2,
        opWarns.size(),
        () -> "exactly one operator WARN per distinct key; got " + opWarns.size()
      );
    } finally {
      julLogger.removeHandler(handler);
      julLogger.setLevel(originalLevel);
      julLogger.setUseParentHandlers(originalUseParent);
    }
  }

  @Test
  void unroutedSinkKeyWarnsOncePerDistinctKey() {
    final var julLogger = Logger.getLogger(MessageProcessorRegistry.class.getName());
    final var captured = new CopyOnWriteArrayList<LogRecord>();
    final var handler = capturingHandler(captured);
    final var originalLevel = julLogger.getLevel();
    final var originalUseParent = julLogger.getUseParentHandlers();
    julLogger.addHandler(handler);
    julLogger.setLevel(Level.ALL);
    julLogger.setUseParentHandlers(false);

    try {
      final var registry = new MessageProcessorRegistry();
      final var keyA = RegistryKey.of("missingSinkA", Object.class);
      final var keyB = RegistryKey.of("missingSinkB", Object.class);

      final var sinkA = registry.getSink(keyA);
      for (int i = 0; i < 50; i++) sinkA.accept("v" + i);
      final var sinkB = registry.getSink(keyB);
      for (int i = 0; i < 50; i++) sinkB.accept("v" + i);

      final var sinkWarns = captured
        .stream()
        .filter(r -> r.getMessage() != null && r.getMessage().contains("No sink found in registry for key"))
        .toList();
      assertEquals(
        2,
        sinkWarns.size(),
        () -> "exactly one sink WARN per distinct key; got " + sinkWarns.size()
      );
    } finally {
      julLogger.removeHandler(handler);
      julLogger.setLevel(originalLevel);
      julLogger.setUseParentHandlers(originalUseParent);
    }
  }

  @Test
  void operatorAndSinkNamespacesDedupIndependently() {
    // The same key unrouted on both the operator and sink side must warn once on each side —
    // the two lookups are independent, so a shared dedup set would wrongly swallow the second.
    final var julLogger = Logger.getLogger(MessageProcessorRegistry.class.getName());
    final var captured = new CopyOnWriteArrayList<LogRecord>();
    final var handler = capturingHandler(captured);
    final var originalLevel = julLogger.getLevel();
    final var originalUseParent = julLogger.getUseParentHandlers();
    julLogger.addHandler(handler);
    julLogger.setLevel(Level.ALL);
    julLogger.setUseParentHandlers(false);

    try {
      final var registry = new MessageProcessorRegistry();
      final var key = RegistryKey.of("sharedName", Object.class);

      registry.getOperator(key).apply("v");
      registry.getOperator(key).apply("v");
      registry.getSink(key).accept("v");
      registry.getSink(key).accept("v");

      final var opWarns = captured
        .stream()
        .filter(r -> r.getMessage() != null && r.getMessage().contains("No operator registered under key"))
        .count();
      final var sinkWarns = captured
        .stream()
        .filter(r -> r.getMessage() != null && r.getMessage().contains("No sink found in registry for key"))
        .count();
      assertEquals(1, opWarns, "operator side warns once");
      assertEquals(1, sinkWarns, "sink side warns once");
    } finally {
      julLogger.removeHandler(handler);
      julLogger.setLevel(originalLevel);
      julLogger.setUseParentHandlers(originalUseParent);
    }
  }

  private static Handler capturingHandler(final List<LogRecord> sink) {
    return new Handler() {
      @Override
      public void publish(final LogRecord record) {
        if (record.getLevel().intValue() >= Level.WARNING.intValue()) sink.add(record);
      }

      @Override
      public void flush() {}

      @Override
      public void close() {}
    };
  }
}
