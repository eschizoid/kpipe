package io.github.eschizoid.kpipe.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.eschizoid.kpipe.registry.FrayScenariosCore;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// The fanout-delegation race.
///
/// Two records fan out through one composite to a shared downstream sink at the same time. Fanout
/// is best-effort about *failures* — a throwing sink is logged and suppressed so the others still
/// run — but it is not best-effort about *delivery*: every record must reach every sink. A
/// delegation dropped under concurrency would lose a record with no error anywhere, which is the
/// silent-failure shape the pipeline's explicit outcome types exist to rule out.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class CompositeMessageSinkFrayTest {

  @FrayTest(iterations = 500)
  void concurrentSendsBothReachTheDownstreamSink() {
    final var downstreamInvocations = new AtomicLong();
    final MessageSink<String> counting = _ -> downstreamInvocations.incrementAndGet();
    final var composite = new CompositeMessageSink<>(List.of(counting));

    FrayScenariosCore.runConcurrently(() -> composite.accept("a"), () -> composite.accept("b"));

    assertEquals(2L, downstreamInvocations.get(), "a fanout delegation was dropped under concurrency");
  }
}
