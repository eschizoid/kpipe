package io.github.eschizoid.kpipe.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.eschizoid.kpipe.record.RecordContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class TestRecordTest {

  @Test
  void contextPresentsTheRecordAsTheConsumerWould() {
    final var ctx = TestRecord.of("v")
      .withKey("k1")
      .withHeader("tenant", "acme")
      .withHeader("tenant", "beta")
      .withHeaderBytes("empty", null)
      .withTimestamp(42L)
      .context();

    assertEquals(TestRecord.DEFAULT_TOPIC, ctx.topic());
    assertEquals(0, ctx.partition());
    assertEquals(0L, ctx.offset());
    assertEquals(42L, ctx.timestamp());
    assertEquals("k1", ctx.keyString());
    assertEquals(Optional.of("beta"), ctx.headerString("tenant"), "the last value wins");
    assertEquals(Optional.empty(), ctx.header("empty"));
    final var all = new ArrayList<String>();
    ctx.forEachHeader((name, value) -> all.add(name + "=" + (value == null ? null : new String(value, UTF_8))));
    assertEquals(List.of("tenant=acme", "tenant=beta", "empty=null"), all);
  }

  @Test
  void aPlainRecordHasNoKeyHeadersOrTimestamp() {
    final var ctx = TestRecord.of("v").context();
    assertNull(ctx.key());
    assertEquals(-1L, ctx.timestamp());
    assertEquals(Optional.empty(), ctx.header("anything"));
  }

  @Test
  void eachWithMethodReturnsANewRecord() {
    final var plain = TestRecord.of("v");
    final var keyed = plain.withKey("k");
    assertNull(plain.context().key());
    assertEquals("k", keyed.context().keyString());
    assertNull(keyed.withKey(null).context().key());
    assertSame("v", keyed.value());
  }

  @Test
  void theRecordCopiesTheBytesItIsGiven() {
    final var key = "k".getBytes(UTF_8);
    final var header = "h".getBytes(UTF_8);
    final var record = TestRecord.of("v").withKeyBytes(key).withHeaderBytes("name", header);
    key[0] = 'x';
    header[0] = 'x';
    assertArrayEquals("k".getBytes(UTF_8), record.context().key());
    assertEquals(Optional.of("h"), record.context().headerString("name"));
  }

  /// The context drives an operator in a unit test, through `RecordContext.callWith`.
  @Test
  void theContextRunsAnOperatorThatReadsTheRecord() {
    final UnaryOperator<String> tag = value -> value + "@" + RecordContext.current().keyString();
    final var record = TestRecord.of("order").withKey("k1");
    assertEquals("order@k1", RecordContext.callWith(record.context(), () -> tag.apply(record.value())));
  }

  @Test
  void aNullValueIsRejected() {
    assertThrows(NullPointerException.class, () -> TestRecord.of(null));
  }

  @Test
  void aNullHeaderStringIsAHeaderWithANullValue() {
    final var ctx = TestRecord.of("v").withHeader("h", null).context();
    assertEquals(Optional.empty(), ctx.header("h"));
    final var names = new ArrayList<String>();
    ctx.forEachHeader((name, _) -> names.add(name));
    assertEquals(List.of("h"), names);
  }
}
