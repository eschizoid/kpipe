package io.github.eschizoid.kpipe.registry;

import java.util.concurrent.atomic.AtomicBoolean;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

/// Concurrency-stress check that the operator and sink namespaces stay independent under a
/// same-key registration race.
///
/// The registry keeps operators and sinks in two separate maps that share the same key shape.
/// A key may exist in one namespace, the other, both, or neither. The hazard this verifies is
/// cross-namespace clobber: if the two namespaces ever shared backing state, registering an
/// operator under a key while concurrently registering a sink under the very same key could
/// lose one of the two registrations.
///
/// jcstress runs the two actors below against fresh state under every interleaving its
/// scheduler can produce, then evaluates the arbiter once both actors have finished. One actor
/// registers an operator under the shared key; the other registers a sink under that same key.
/// The arbiter then looks up both namespaces through the live `getOperator` / `getSink`
/// wrappers and exercises each: a registered operator transforms a sentinel input, and a
/// registered sink flips a flag when invoked. A missing entry falls back to the logging
/// pass-through (operator) or logging drop (sink), neither of which produces the registered
/// effect, so the probes distinguish "my registration survived" from "I got the fallback".
///
/// Both probes must report success regardless of interleaving. r1 is 1 when the operator
/// applied its transform, r2 is 1 when the sink received the value. Anything other than (1, 1)
/// means a registration was lost or one namespace clobbered the other.
@JCStressTest
@Outcome(id = "1, 1", expect = Expect.ACCEPTABLE, desc = "Both namespaces survived the same-key race.")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "A registration was lost or namespaces clobbered each other.")
@State
public class MessageProcessorRegistryJCStressTest {

  private static final RegistryKey<Integer> KEY = RegistryKey.of("shared", Integer.class);

  private final MessageProcessorRegistry registry = new MessageProcessorRegistry();
  private final AtomicBoolean sinkReceived = new AtomicBoolean(false);

  @Actor
  public void registerOperator() {
    // Identity-plus-one transform: a surviving registration is observable because the logging
    // pass-through fallback returns the input unchanged.
    registry.registerOperator(KEY, input -> input + 1);
  }

  @Actor
  public void registerSink() {
    registry.registerSink(KEY, value -> sinkReceived.set(true));
  }

  @Arbiter
  public void observe(final II_Result r) {
    // Live lookups read the current map entry at invocation time. The operator probe feeds a
    // sentinel and checks the transform fired; the sink probe checks the value was delivered.
    final var transformed = registry.getOperator(KEY).apply(41);
    r.r1 = transformed == 42 ? 1 : 0;

    registry.getSink(KEY).accept(7);
    r.r2 = sinkReceived.get() ? 1 : 0;
  }
}
