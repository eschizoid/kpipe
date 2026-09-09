package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// The registry namespace race.
///
/// Operators and sinks live in separate maps under the same key shape, so registering one of each
/// under an identical key must leave both intact. If the two namespaces shared a map — or a
/// registration read-modify-wrote a shared structure without atomicity — one would clobber the
/// other and the loser's lookup would silently fall back to a pass-through.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class MessageProcessorRegistryFrayTest {

  private static final RegistryKey<Integer> KEY = RegistryKey.of("shared", Integer.class);

  @FrayTest(iterations = 500)
  void operatorAndSinkRegistrationsDoNotClobberEachOther() {
    final var registry = new MessageProcessorRegistry();
    final var sinkReceived = new AtomicBoolean(false);

    FrayScenariosCore.runConcurrently(
      // Identity-plus-one: a surviving registration is observable because the pass-through
      // fallback would return the input unchanged.
      () -> registry.registerOperator(KEY, input -> input + 1),
      () -> registry.registerSink(KEY, value -> sinkReceived.set(true))
    );

    assertEquals(42, registry.getOperator(KEY).apply(41), "the registered operator did not survive the race");
    registry.getSink(KEY).accept(7);
    assertTrue(sinkReceived.get(), "the registered sink did not survive the race");
  }
}
