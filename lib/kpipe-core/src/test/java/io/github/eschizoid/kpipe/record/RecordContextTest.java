package io.github.eschizoid.kpipe.record;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
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

  @Test
  void rekeyReplacesTheKeyAndKeepsTheOriginal() {
    final var seen = RecordContext.callWith(ORDER, () -> {
      final var value = RecordContext.<String>rekey(v -> (v + "-key").getBytes(UTF_8)).apply("order");
      return (
        value +
        "|" +
        RecordContext.current().keyString() +
        "|" +
        new String(RecordContext.current().originalKey(), UTF_8)
      );
    });
    assertEquals("order|order-key|k1", seen);
  }

  @Test
  void theLastRekeyWinsAndANullKeyMeansNoKey() {
    RecordContext.callWith(ORDER, () -> {
      RecordContext.<String>rekey(_ -> "a".getBytes(UTF_8)).apply("v");
      RecordContext.<String>rekey(_ -> "b".getBytes(UTF_8)).apply("v");
      assertEquals("b", RecordContext.current().keyString());
      RecordContext.<String>rekey(_ -> null).apply("v");
      assertNull(RecordContext.current().key());
      assertEquals("k1", new String(RecordContext.current().originalKey(), UTF_8));
      return null;
    });
  }

  @Test
  void withHeaderAddsAHeaderAfterTheRecordsOwn() {
    RecordContext.callWith(ORDER, () -> {
      RecordContext.<String>withHeader("tenant", _ -> "beta".getBytes(UTF_8)).apply("v");
      RecordContext.<String>withHeader("source", _ -> "api".getBytes(UTF_8)).apply("v");
      final var ctx = RecordContext.current();
      assertEquals(Optional.of("beta"), ctx.headerString("tenant"));
      assertEquals(Optional.of("api"), ctx.headerString("source"));
      final var all = new ArrayList<String>();
      ctx.forEachHeader((name, value) -> all.add(name + "=" + new String(value, UTF_8)));
      assertEquals(List.of("tenant=acme", "tenant=beta", "source=api"), all);
      return null;
    });
    assertEquals(Optional.of("acme"), ORDER.headerString("tenant"), "the consumed record is unchanged");
  }

  @Test
  void bindingAgainStartsFromTheConsumedRecord() {
    RecordContext.callWith(ORDER, () -> {
      RecordContext.<String>rekey(_ -> "changed".getBytes(UTF_8)).apply("v");
      RecordContext.<String>withHeader("added", _ -> new byte[0]).apply("v");
      return null;
    });
    RecordContext.callWith(ORDER, () -> {
      assertEquals("k1", RecordContext.current().keyString());
      assertEquals(Optional.empty(), RecordContext.current().header("added"));
      return null;
    });
  }

  @Test
  void aCapturedContextKeepsTheKeyItHadWhenCaptured() {
    RecordContext.callWith(ORDER, () -> {
      final var before = RecordContext.current();
      RecordContext.<String>rekey(_ -> "new".getBytes(UTF_8)).apply("v");
      final var after = RecordContext.current();
      assertEquals("k1", before.keyString());
      assertEquals("new", after.keyString());
      return null;
    });
  }

  @Test
  void rekeyAndWithHeaderRunOnlyInsideAPipeline() {
    final var rekey = assertThrows(IllegalStateException.class, () ->
      RecordContext.<String>rekey(_ -> null).apply("v")
    );
    assertTrue(rekey.getMessage().startsWith("rekey changes the record being processed"), rekey.getMessage());
    final var header = assertThrows(IllegalStateException.class, () ->
      RecordContext.callWithout(() -> RecordContext.<String>withHeader("h", _ -> null).apply("v"))
    );
    assertTrue(header.getMessage().startsWith("withHeader changes the record being processed"), header.getMessage());
  }

  /// Each change builds on the ones before it, in either order.
  @Test
  void rekeyAndWithHeaderKeepEachOthersChanges() {
    RecordContext.callWith(ORDER, () -> {
      RecordContext.<String>withHeader("first", _ -> "1".getBytes(UTF_8)).apply("v");
      RecordContext.<String>rekey(_ -> "k2".getBytes(UTF_8)).apply("v");
      RecordContext.<String>withHeader("second", _ -> "2".getBytes(UTF_8)).apply("v");
      final var ctx = RecordContext.current();
      assertEquals("k2", ctx.keyString());
      assertEquals(Optional.of("1"), ctx.headerString("first"));
      assertEquals(Optional.of("2"), ctx.headerString("second"));
      return null;
    });
  }

  /// A captured context bound again on another thread keeps its own changes: neither binding
  /// sees the other's rekey.
  @Test
  void bindingACapturedContextOnAnotherThreadLeavesThisBindingAlone() throws Exception {
    final var exec = Executors.newSingleThreadExecutor();
    try {
      RecordContext.callWith(ORDER, () -> {
        final var captured = RecordContext.current();
        RecordContext.<String>rekey(_ -> "here".getBytes(UTF_8)).apply("v");
        final var there = exec
          .submit(() ->
            RecordContext.callWith(captured, () -> {
              final var before = RecordContext.current().keyString();
              RecordContext.<String>rekey(_ -> "there".getBytes(UTF_8)).apply("v");
              return before + "->" + RecordContext.current().keyString();
            })
          )
          .get();
        assertEquals("k1->there", there, "the other binding starts from the captured context");
        assertEquals("here", RecordContext.current().keyString(), "this binding keeps its own rekey");
        return null;
      });
    } finally {
      exec.shutdown();
    }
  }

  /// A rekey inside a nested binding ends with it.
  @Test
  void aRekeyInsideANestedBindingEndsWithIt() {
    RecordContext.callWith(ORDER, () -> {
      RecordContext.callWith(RecordContext.current(), () ->
        RecordContext.<String>rekey(_ -> "inner".getBytes(UTF_8)).apply("v")
      );
      assertEquals("k1", RecordContext.current().keyString());
      return null;
    });
  }

  /// A changed context still answers everything else from the consumed record.
  @Test
  void aChangedContextKeepsTheRecordsOtherFields() {
    RecordContext.callWith(ORDER, () -> {
      assertTrue(RecordContext.isBound());
      RecordContext.<String>rekey(_ -> "k2".getBytes(UTF_8)).apply("v");
      final var rekeyed = RecordContext.current();
      final var headers = new ArrayList<String>();
      rekeyed.forEachHeader((name, value) -> headers.add(name + "=" + new String(value, UTF_8)));
      assertEquals(
        List.of("orders", "1", "42", "7", "k1", "[tenant=acme]"),
        List.of(
          rekeyed.topic(),
          "" + rekeyed.partition(),
          "" + rekeyed.offset(),
          "" + rekeyed.timestamp(),
          new String(rekeyed.originalKey(), UTF_8),
          headers.toString()
        )
      );
      RecordContext.<String>withHeader("h", _ -> new byte[0]).apply("v");
      assertEquals("k2", RecordContext.current().keyString());
      assertEquals(Optional.of("acme"), RecordContext.current().headerString("tenant"));
      return null;
    });
  }
}
