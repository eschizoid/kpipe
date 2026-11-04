package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.sink.MessageSink;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/// The builder's operator and sink methods behave as their `Stream` namesakes do.
class TypedPipelineBuilderTest {

  private final MessageProcessorRegistry registry = new MessageProcessorRegistry();

  @Test
  void filterKeepsMatchingRecordsAndFiltersTheRest() {
    final var pipeline = registry
      .pipeline(MessageFormat.bytes())
      .filter(value -> value[0] > 0)
      .build();

    assertInstanceOf(Result.Passed.class, pipeline.process(new byte[] { 1 }));
    assertInstanceOf(Result.Filtered.class, pipeline.process(new byte[] { 0 }));
  }

  @Test
  void peekRunsTheSideEffectAndPassesTheRecordOn() {
    final var seen = new ArrayList<Byte>();
    final var pipeline = registry
      .pipeline(MessageFormat.bytes())
      .peek(value -> seen.add(value[0]))
      .build();

    final var passed = assertInstanceOf(Result.Passed.class, pipeline.process(new byte[] { 7 }));
    assertArrayEquals(new byte[] { 7 }, (byte[]) passed.value());
    assertEquals(List.of((byte) 7), seen);
  }

  @Test
  void aSecondSinkIsRejected() {
    final MessageSink<byte[]> sink = _ -> {};
    final var builder = registry.pipeline(MessageFormat.bytes()).toSink(sink);

    final var thrown = assertThrows(IllegalStateException.class, () -> builder.toSink(sink));
    assertTrue(thrown.getMessage().contains("toMulti"), thrown.getMessage());
    assertThrows(IllegalStateException.class, () -> builder.toMulti(sink, sink));
  }

  @Test
  void toMultiDeliversToEverySinkAndCarriesOnPastOneThatThrows() {
    final var delivered = new ArrayList<String>();
    final var pipeline = registry
      .pipeline(MessageFormat.bytes())
      .toMulti(
        _ -> delivered.add("first"),
        _ -> {
          throw new IllegalStateException("down");
        },
        _ -> delivered.add("third")
      )
      .build();

    assertDoesNotThrow(() -> pipeline.getSink().accept(new byte[] { 1 }));
    assertEquals(List.of("first", "third"), delivered);
  }

  @Test
  void toMultiByKeyRejectsAKeyWithNothingRegistered() {
    final var registered = RegistryKey.of("audit", byte[].class);
    registry.registerSink(registered, _ -> {});
    final var builder = registry.pipeline(MessageFormat.bytes());

    assertThrows(IllegalArgumentException.class, () ->
      builder.toMulti(registered, RegistryKey.of("missing", byte[].class))
    );
    assertDoesNotThrow(() -> builder.toSink(registered), "the failed toMulti left the builder without a sink");
  }
}
