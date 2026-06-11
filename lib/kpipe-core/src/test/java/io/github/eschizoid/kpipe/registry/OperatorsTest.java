package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

/// Verifies the operator factories in [Operators] behave as documented:
/// - filter/drop return null to signal intentional filtering
/// - tap runs side-effects and passes the value through unchanged
class OperatorsTest {

  @Test
  void filterShouldKeepWhenPredicateTrue() {
    final var op = Operators.filter((String s) -> s.startsWith("keep-"));
    assertEquals("keep-me", op.apply("keep-me"));
  }

  @Test
  void filterShouldReturnNullWhenPredicateFalse() {
    final var op = Operators.filter((String s) -> s.startsWith("keep-"));
    assertNull(op.apply("drop-me"));
  }

  @Test
  void dropShouldReturnNullWhenPredicateTrue() {
    final var op = Operators.drop((String s) -> s.startsWith("drop-"));
    assertNull(op.apply("drop-me"));
  }

  @Test
  void dropShouldKeepWhenPredicateFalse() {
    final var op = Operators.drop((String s) -> s.startsWith("drop-"));
    assertEquals("keep-me", op.apply("keep-me"));
  }

  @Test
  void peekShouldRunSideEffectAndPassThrough() {
    final var captured = new AtomicReference<String>();
    final var op = Operators.<String>peek(captured::set);

    final var result = op.apply("original");

    assertSame("original", result);
    assertEquals("original", captured.get());
  }

  @Test
  void mapShouldApplyFunction() {
    final var op = Operators.map((String s) -> s.toUpperCase());
    assertEquals("HELLO", op.apply("hello"));
  }

  @Test
  void filterShouldComposeInPipeline() {
    // Verify the operator actually integrates with TypedPipelineBuilder's
    // null-handling — filtered messages short-circuit the chain.
    final var registry = new MessageProcessorRegistry();
    final var pipeline = registry
      .pipeline(MessageFormat.bytes())
      .add(Operators.filter(b -> new String(b).startsWith("keep-")))
      .add(
        Operators.peek(b -> {
          // This should not be invoked for filtered messages.
          if (new String(b).startsWith("drop-")) throw new AssertionError("filter did not short-circuit");
        })
      )
      .build();

    assertInstanceOf(Result.Filtered.class, pipeline.process("drop-this".getBytes()));
    final var keptResult = pipeline.process("keep-this".getBytes());
    final var passed = assertInstanceOf(Result.Passed.class, keptResult);
    assertEquals("keep-this", new String((byte[]) passed.value()));
  }

  @Test
  void safeShouldReturnInputOnException() {
    final UnaryOperator<String> throwing = _ -> {
      throw new RuntimeException("boom");
    };
    final var op = Operators.safe(throwing);
    assertEquals("input", op.apply("input"));
  }

  @Test
  void requireFieldShouldKeepWhenPresent() {
    final var op = Operators.requireField("id");
    final Map<String, Object> msg = new HashMap<>();
    msg.put("id", 42);
    assertSame(msg, op.apply(msg));
  }

  @Test
  void requireFieldShouldFilterWhenMissing() {
    final var op = Operators.requireField("id");
    final Map<String, Object> msg = new HashMap<>();
    msg.put("name", "alice");
    assertNull(op.apply(msg));
  }

  @Test
  void renameShouldMoveKey() {
    final var op = Operators.rename("ts", "timestamp");
    final Map<String, Object> msg = new HashMap<>();
    msg.put("ts", 1234L);
    msg.put("name", "alice");

    final var result = op.apply(msg);

    assertSame(msg, result);
    assertFalse(result.containsKey("ts"));
    assertEquals(1234L, result.get("timestamp"));
    assertEquals("alice", result.get("name"));
  }

  @Test
  void renameShouldNoopWhenSourceMissing() {
    final var op = Operators.rename("ts", "timestamp");
    final Map<String, Object> msg = new HashMap<>();
    msg.put("name", "alice");

    final var result = op.apply(msg);

    assertSame(msg, result);
    assertFalse(result.containsKey("timestamp"));
    assertEquals("alice", result.get("name"));
  }

  @Test
  void composeShouldChainInOrder() {
    final UnaryOperator<String> addA = s -> s + "a";
    final UnaryOperator<String> addB = s -> s + "b";

    final var op = Operators.compose(addA, addB);

    assertEquals("xab", op.apply("x"));
  }

  @Test
  void composeShouldShortCircuitOnNull() {
    final var downstreamCalled = new AtomicBoolean(false);

    final UnaryOperator<String> first = s -> s + "a";
    final UnaryOperator<String> filter = _ -> null;
    final UnaryOperator<String> downstream = s -> {
      downstreamCalled.set(true);
      return s + "b";
    };

    final var op = Operators.compose(first, filter, downstream);

    assertNull(op.apply("x"));
    assertFalse(downstreamCalled.get(), "downstream operator must not be called after a null result");
  }

  @Test
  void composeWithEmptyArrayShouldBeIdentity() {
    final UnaryOperator<String> op = Operators.compose();
    assertEquals("hello", op.apply("hello"));
  }

  @Test
  void removeFieldsShouldDropAllListed() {
    final var op = Operators.removeFields("password", "secret");
    final Map<String, Object> msg = new HashMap<>();
    msg.put("id", 1);
    msg.put("password", "p");
    msg.put("secret", "s");
    msg.put("keep", "yes");

    final var result = op.apply(msg);

    assertSame(msg, result);
    assertFalse(result.containsKey("password"));
    assertFalse(result.containsKey("secret"));
    assertTrue(result.containsKey("id"));
    assertTrue(result.containsKey("keep"));
  }

  @Test
  void removeFieldsShouldHandleNullInput() {
    final var op = Operators.removeFields("password");
    assertNull(op.apply(null));
  }
}
