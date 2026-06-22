package io.github.eschizoid.kpipe.schemaregistry.confluent;

import io.github.eschizoid.kpipe.registry.SchemaResolver;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.L_Result;

/// Concurrency-stress check that two concurrent cache misses on the same schema ID collapse to
/// exactly one underlying load, and that both callers see the same resolved value.
///
/// jcstress runs the two actors below against fresh state under every interleaving its scheduler
/// can produce, then evaluates the arbiter once both actors have finished. This reaches the
/// interleaving that a single-threaded sequence test cannot: in production the resolver is shared
/// across many virtual-thread workers that can miss on the same uncached ID at the same instant.
///
/// Scenario. The state wraps a counting fake resolver — it records how many times the underlying
/// load runs and returns a fixed schema string for the target ID. Both actors call lookupById on
/// the same ID concurrently, so they race into the same empty cache bucket. The wrapper uses
/// computeIfAbsent, whose per-bucket lock must serialize the two misses: the loser blocks until
/// the winner populates the entry, so the slow load runs exactly once and both actors return the
/// winner's value.
///
/// The arbiter folds three facts into one verdict string: the underlying load count, whether both
/// actors returned the identical instance, and whether that instance equals the fixed schema. The
/// only acceptable outcome is one load with two matching results. Any other load count, or any
/// divergence between the two returned values, would mean computeIfAbsent failed to collapse the
/// concurrent misses.
@JCStressTest
@Outcome(
    id = "loads=1,sameRef=true,value=ok",
    expect = Expect.ACCEPTABLE,
    desc = "Concurrent misses collapsed to one load; both callers got the same resolved schema.")
@Outcome(
    id = ".*",
    expect = Expect.FORBIDDEN,
    desc = "Duplicate load or divergent results — computeIfAbsent did not collapse the misses.")
@State
public class CachedSchemaResolverJCStressTest {

  private static final int SCHEMA_ID = 42;
  private static final String SCHEMA_JSON = "{\"type\":\"string\"}";

  private final AtomicInteger loadCount = new AtomicInteger();
  private final CachedSchemaResolver resolver;

  private volatile String result1;
  private volatile String result2;

  public CachedSchemaResolverJCStressTest() {
    // Counting fake: every underlying load bumps the counter and hands back the same instance,
    // so a duplicate load shows up as a count above one and a fresh reference comparison.
    final SchemaResolver counting =
        id -> {
          loadCount.incrementAndGet();
          return SCHEMA_JSON;
        };
    resolver = new CachedSchemaResolver(counting);
  }

  @Actor
  public void resolverOne() {
    result1 = resolver.lookupById(SCHEMA_ID);
  }

  @Actor
  public void resolverTwo() {
    result2 = resolver.lookupById(SCHEMA_ID);
  }

  @Arbiter
  public void verdict(final L_Result r) {
    final var sameRef = result1 == result2;
    final var bothOk = SCHEMA_JSON.equals(result1) && SCHEMA_JSON.equals(result2);
    r.r1 =
        "loads="
            + loadCount.get()
            + ",sameRef="
            + sameRef
            + ",value="
            + (bothOk ? "ok" : "bad");
  }
}
