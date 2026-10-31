package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import org.junit.jupiter.api.Test;

/// A key with nothing registered under it fails, at build time when the pipeline is built and as
/// a failed record when it is unregistered later, instead of passing records on untransformed or
/// dropping them with their offsets committed.
class MessageProcessorRegistryMissingKeyTest {

  private static final RegistryKey<byte[]> KEY = RegistryKey.of("enrich", byte[].class);

  private final MessageProcessorRegistry registry = new MessageProcessorRegistry();

  @Test
  void addingAnOperatorKeyWithNothingRegisteredFailsTheBuild() {
    final var builder = registry.pipeline(MessageFormat.bytes());

    final var thrown = assertThrows(IllegalArgumentException.class, () -> builder.add(KEY));
    assertTrue(thrown.getMessage().contains("enrich"), thrown.getMessage());
  }

  @Test
  void addingASinkKeyWithNothingRegisteredFailsTheBuild() {
    final var builder = registry.pipeline(MessageFormat.bytes());

    final var thrown = assertThrows(IllegalArgumentException.class, () -> builder.toSink(KEY));
    assertTrue(thrown.getMessage().contains("enrich"), thrown.getMessage());
  }

  @Test
  void anOperatorUnregisteredAfterTheBuildFailsTheRecord() {
    registry.registerOperator(KEY, value -> value);
    final var pipeline = registry.pipeline(MessageFormat.bytes()).add(KEY).build();
    registry.unregisterOperator(KEY);

    final var failed = assertInstanceOf(Result.Failed.class, pipeline.process(new byte[] { 1 }));
    assertInstanceOf(IllegalStateException.class, failed.cause());
  }

  @Test
  void aSinkUnregisteredAfterTheBuildThrowsWhenItRuns() {
    registry.registerSink(KEY, _ -> {});
    final var pipeline = registry.pipeline(MessageFormat.bytes()).toSink(KEY).build();
    registry.unregisterSink(KEY);

    assertThrows(IllegalStateException.class, () -> pipeline.getSink().accept(new byte[] { 1 }));
  }

  @Test
  void reRegisteringAKeyTakesEffectInABuiltPipeline() {
    final var delivered = new ArrayList<String>();
    registry.registerOperator(KEY, _ -> new byte[] { 1 });
    registry.registerSink(KEY, value -> delivered.add("first:" + value[0]));
    final var pipeline = registry.pipeline(MessageFormat.bytes()).add(KEY).toSink(KEY).build();

    registry.registerOperator(KEY, _ -> new byte[] { 2 });
    registry.registerSink(KEY, value -> delivered.add("second:" + value[0]));
    final var passed = assertInstanceOf(Result.Passed.class, pipeline.process(new byte[] { 0 }));
    pipeline.getSink().accept((byte[]) passed.value());

    assertEquals(java.util.List.of("second:2"), delivered);
  }
}
