package io.github.eschizoid.kpipe.record;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;

class RecordContextTest {

  private static final class Fixed extends RecordContext {

    private final byte[] key;
    private final Map<String, byte[]> headers;

    Fixed(final byte[] key, final Map<String, byte[]> headers) {
      this.key = key;
      this.headers = headers;
    }

    @Override
    public String topic() {
      return "orders";
    }

    @Override
    public int partition() {
      return 1;
    }

    @Override
    public long offset() {
      return 42L;
    }

    @Override
    public long timestamp() {
      return 7L;
    }

    @Override
    public byte[] key() {
      return key;
    }

    @Override
    public byte[] originalKey() {
      return key;
    }

    @Override
    public Optional<byte[]> header(final String name) {
      return Optional.ofNullable(headers.get(name));
    }

    @Override
    public void forEachHeader(final BiConsumer<String, byte[]> action) {
      headers.forEach(action);
    }
  }

  private static final RecordContext ORDER = new Fixed("k1".getBytes(UTF_8), Map.of("tenant", "acme".getBytes(UTF_8)));

  @Test
  void currentThrowsAClearMessageWhenNoRecordIsBound() {
    final var e = assertThrows(IllegalStateException.class, RecordContext::current);
    assertTrue(e.getMessage().startsWith("No record is being processed on this thread."), e.getMessage());
    assertTrue(e.getMessage().contains("var ctx = RecordContext.current()"), e.getMessage());
    assertFalse(RecordContext.isBound());
  }

  @Test
  void callWithBindsTheContextForTheCallOnly() {
    final var seen = RecordContext.callWith(ORDER, RecordContext::current);
    assertSame(ORDER, seen);
    assertFalse(RecordContext.isBound(), "the binding ends with the call");
  }

  @Test
  void anInnerBindingWinsAndTheOuterOneComesBack() {
    final var inner = new Fixed(null, Map.of());
    RecordContext.callWith(ORDER, () -> {
      assertSame(inner, RecordContext.callWith(inner, RecordContext::current));
      assertSame(ORDER, RecordContext.current());
      return null;
    });
  }

  @Test
  void callWithoutHidesTheRecordAndTheOuterOneComesBack() {
    RecordContext.callWith(ORDER, () -> {
      RecordContext.callWithout(() -> {
        assertFalse(RecordContext.isBound());
        assertThrows(IllegalStateException.class, RecordContext::current);
        return null;
      });
      assertSame(ORDER, RecordContext.current());
      return null;
    });
  }

  @Test
  void aThreadStartedInsideTheCallDoesNotSeeTheRecord() throws Exception {
    final var bound = new AtomicReference<Boolean>();
    RecordContext.callWith(ORDER, () -> {
      Thread.ofVirtual()
        .start(() -> bound.set(RecordContext.isBound()))
        .join();
      return null;
    });
    assertEquals(false, bound.get());
  }

  @Test
  void callWithPassesACheckedExceptionThrough() {
    final var failure = new IOException("boom");
    final var thrown = assertThrows(IOException.class, () ->
      RecordContext.callWith(ORDER, () -> {
        throw failure;
      })
    );
    assertSame(failure, thrown);
  }

  @Test
  void callWithRejectsANullContext() {
    assertThrows(NullPointerException.class, () -> RecordContext.callWith(null, () -> null));
  }

  @Test
  void stringAccessorsDecodeUtf8() {
    assertEquals("k1", ORDER.keyString());
    assertEquals(Optional.of("acme"), ORDER.headerString("tenant"));
    assertEquals(Optional.empty(), ORDER.headerString("missing"));
    assertNull(new Fixed(null, Map.of()).keyString());
  }
}
