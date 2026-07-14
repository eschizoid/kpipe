package io.github.eschizoid.kpipe.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Proves the [CrashRestartHarness] itself pins at-least-once re-delivery across a crash:
/// consumer A processes `[0, P)` and commits `k`, then B resumes from `k` and must
/// re-deliver the uncommitted tail `[k, P)`. Runs across every [ProcessingMode] plus a
/// batch cell, all Docker-free.
class CrashRestartHarnessTest {

  /// A value-equal `String` format (UTF-8) so set/containment assertions are exact — unlike raw
  /// `byte[]`, which compares by identity.
  private static final MessageFormat<String> STRING_FORMAT = new MessageFormat<>() {
    @Override
    public byte[] serialize(final String data) {
      return data.getBytes(UTF_8);
    }

    @Override
    public String deserialize(final byte[] data) {
      return new String(data, UTF_8);
    }
  };

  private static List<String> values(final int n) {
    return IntStream.range(0, n)
      .mapToObj(i -> "v" + i)
      .toList();
  }

  private static Set<String> setOf(final int fromInclusive, final int toExclusive) {
    return IntStream.range(fromInclusive, toExclusive)
      .mapToObj(i -> "v" + i)
      .collect(Collectors.toSet());
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Re-delivery across all three processing modes. N=10, k=3, P=7.
  // ────────────────────────────────────────────────────────────────────────────────

  @ParameterizedTest
  @EnumSource(ProcessingMode.class)
  void uncommittedTailIsRedeliveredOnRestart(final ProcessingMode mode) {
    final var result = CrashRestartHarness.builder(STRING_FORMAT)
      .withProcessingMode(mode)
      .seed(values(10))
      .commitUpTo(3)
      .crashAfter(7)
      .restart();

    // A saw the processed prefix [0,7); B resumed over [3,10).
    assertEquals(setOf(0, 7), Set.copyOf(result.firstRun()), "A must process the seeded prefix [0,7)");
    assertEquals(setOf(3, 10), Set.copyOf(result.secondRun()), "B must resume over [3,10)");

    // The uncommitted tail [3,7) is the at-least-once obligation.
    assertEquals(setOf(3, 7), Set.copyOf(result.redeliveredTail()), "the uncommitted tail is [3,7)");
    assertTrue(
      Set.copyOf(result.secondRun()).containsAll(result.redeliveredTail()),
      "B must re-deliver every record in the uncommitted tail [3,7)"
    );

    // No loss: A ∪ B covers the whole stream [0,10).
    final var union = new HashSet<>(result.firstRun());
    union.addAll(result.secondRun());
    assertEquals(setOf(0, 10), union, "A ∪ B must cover every record — no loss across the crash");

    assertEquals(3L, result.committedOffset(), "committed offset is k");
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Operators run in both phases: a transform applied by A is re-applied by B on the tail.
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void pipeOperatorIsAppliedInBothPhases() {
    final var result = CrashRestartHarness.builder(STRING_FORMAT)
      .pipe(String::toUpperCase)
      .seed(values(10))
      .commitUpTo(3)
      .crashAfter(7)
      .restart();

    assertEquals(List.of("V0", "V1", "V2", "V3", "V4", "V5", "V6"), result.firstRun(), "A applies the operator");
    assertEquals(List.of("V3", "V4", "V5", "V6"), result.redeliveredTail(), "tail is transformed too");
    assertEquals(
      List.of("V3", "V4", "V5", "V6", "V7", "V8", "V9"),
      result.secondRun(),
      "B re-applies the operator on the resumed window"
    );
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Batch crash: records buffered (uncommitted) at crash time are re-delivered by B.
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void bufferedBatchRecordsAreRedeliveredAfterCrash() {
    final var result = CrashRestartHarness.builder(STRING_FORMAT)
      .withProcessingMode(ProcessingMode.SEQUENTIAL)
      .seed(values(10))
      .commitUpTo(3)
      .crashAfter(7)
      .toBatchOfSize(4)
      .restart();

    // The trailing partial batch flushes on crash/close, so A still delivers all of [0,7); none of
    // it past k=3 was committed, so B must re-deliver the tail.
    assertEquals(List.of("v0", "v1", "v2", "v3", "v4", "v5", "v6"), result.firstRun(), "A delivers [0,7) in order");
    assertEquals(List.of("v3", "v4", "v5", "v6"), result.redeliveredTail(), "uncommitted tail is [3,7)");
    assertTrue(
      result.secondRun().containsAll(result.redeliveredTail()),
      "B must re-deliver the buffered-at-crash tail [3,7)"
    );
    assertEquals(List.of("v3", "v4", "v5", "v6", "v7", "v8", "v9"), result.secondRun(), "B resumes over [3,10)");
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Geometry validation.
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void commitUpToAtOrAboveCrashAfterIsRejected() {
    final var harness = CrashRestartHarness.builder(STRING_FORMAT).seed(values(10)).commitUpTo(5).crashAfter(3);
    assertThrows(IllegalArgumentException.class, harness::restart);
  }

  @Test
  void crashAfterAboveSeedSizeIsRejected() {
    final var harness = CrashRestartHarness.builder(STRING_FORMAT).seed(values(10)).commitUpTo(1).crashAfter(20);
    assertThrows(IllegalArgumentException.class, harness::restart);
  }

  @Test
  void emptySeedIsRejected() {
    final var harness = CrashRestartHarness.builder(STRING_FORMAT).seed(List.of()).commitUpTo(0).crashAfter(1);
    assertThrows(IllegalArgumentException.class, harness::restart);
  }
}
