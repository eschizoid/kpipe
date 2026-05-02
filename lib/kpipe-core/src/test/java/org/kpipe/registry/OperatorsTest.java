package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.atomic.AtomicReference;
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
  void tapShouldRunSideEffectAndPassThrough() {
    final var captured = new AtomicReference<String>();
    final var op = Operators.tap((String s) -> captured.set(s));

    final var result = op.apply("original");

    assertSame("original", result);
    assertEquals("original", captured.get());
  }

  @Test
  void filterShouldComposeInPipeline() {
    // Verify the operator actually integrates with TypedPipelineBuilder's
    // null-handling — filtered messages short-circuit the chain.
    final var registry = new MessageProcessorRegistry("operators-test");
    final var pipeline = registry
      .pipeline(MessageFormat.bytes())
      .add(Operators.filter(b -> new String(b).startsWith("keep-")))
      .add(
        Operators.tap(b -> {
          // This should not be invoked for filtered messages.
          if (new String(b).startsWith("drop-")) throw new AssertionError("filter did not short-circuit");
        })
      )
      .build();

    assertNull(pipeline.apply("drop-this".getBytes()));
    final var kept = pipeline.apply("keep-this".getBytes());
    assertEquals("keep-this", new String(kept));
  }
}
