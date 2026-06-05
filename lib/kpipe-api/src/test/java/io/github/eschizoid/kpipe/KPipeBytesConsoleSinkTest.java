package io.github.eschizoid.kpipe;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/// Verifies the rendering behavior of [KPipe#bytesConsoleSink()].
///
/// Captures the JUL log output produced by the package-level `KPipe` logger by attaching a
/// custom [Handler] that records each `LogRecord`'s formatted message. The internal helpers
/// `renderBytes` / `looksLikeText` are private, so all assertions go through the public
/// `MessageSink<byte[]>` returned by the factory.
class KPipeBytesConsoleSinkTest {

  private CapturingHandler handler;
  private Logger julLogger;

  @BeforeEach
  void setUp() {
    handler = new CapturingHandler();
    julLogger = Logger.getLogger(KPipe.class.getName());
    julLogger.addHandler(handler);
    julLogger.setUseParentHandlers(false);
  }

  @AfterEach
  void tearDown() {
    julLogger.removeHandler(handler);
    julLogger.setUseParentHandlers(true);
  }

  private String output() {
    return handler.toString();
  }

  @Test
  void nullInputLogsLiteralNull() {
    KPipe.bytesConsoleSink().accept(null);
    assertTrue(output().contains("null"), () -> "Expected 'null' in output, got: " + output());
  }

  @Test
  void emptyByteArrayLogsLiteralEmpty() {
    KPipe.bytesConsoleSink().accept(new byte[0]);
    assertTrue(output().contains("empty"), () -> "Expected 'empty' in output, got: " + output());
  }

  @Test
  void utf8TextRendersAsString() {
    KPipe.bytesConsoleSink().accept("hello, world".getBytes(UTF_8));
    assertTrue(output().contains("hello, world"), () -> "Expected UTF-8 string in output, got: " + output());
  }

  @Test
  void binaryRendersAsHex() {
    final var binary = new byte[] { (byte) 0x00, (byte) 0xFF, (byte) 0x42 };
    KPipe.bytesConsoleSink().accept(binary);
    final var out = output();
    assertTrue(out.contains("0x"), () -> "Expected hex prefix '0x' in output, got: " + out);
    // 0xFF and 0x42 should be present in the hex preview (00 is harder to disambiguate).
    assertTrue(out.toLowerCase().contains("ff"), () -> "Expected 'ff' hex in output, got: " + out);
    assertTrue(out.contains("42"), () -> "Expected '42' hex in output, got: " + out);
  }

  @Test
  void largePayloadIsTruncatedTo64Bytes() {
    // Truncation only applies on the hex/binary path; a leading NUL byte forces `looksLikeText`
    // to return false so the rest of the (otherwise printable) 'a' bytes go through the hex
    // preview. The total length (200) must still appear in the rendered output.
    final var large = new byte[200];
    java.util.Arrays.fill(large, (byte) 'a');
    large[0] = 0x00;
    KPipe.bytesConsoleSink().accept(large);
    final var out = output();
    assertTrue(
      out.contains("(200 bytes total)"),
      () -> "Expected '(200 bytes total)' truncation suffix in output, got: " + out
    );
  }

  /// JUL [Handler] that captures each [LogRecord]'s formatted message into an in-memory buffer.
  /// Handles parameterized messages (e.g. `{0}`) via [MessageFormat#format(Object)].
  private static final class CapturingHandler extends Handler {

    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    @Override
    public void publish(final LogRecord record) {
      if (record.getMessage() == null) return;
      final var params = record.getParameters();
      final var formatted = (params != null && params.length > 0)
        ? MessageFormat.format(record.getMessage(), params)
        : record.getMessage();
      buffer.writeBytes(formatted.getBytes(StandardCharsets.UTF_8));
      buffer.writeBytes("\n".getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    @Override
    public String toString() {
      return buffer.toString(StandardCharsets.UTF_8);
    }
  }
}
