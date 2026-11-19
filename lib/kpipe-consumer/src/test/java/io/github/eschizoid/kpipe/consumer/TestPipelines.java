package io.github.eschizoid.kpipe.consumer;

import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import java.util.function.UnaryOperator;

/// Test-only fixtures for [MessagePipeline]. Tests that exercise consumer mechanics
/// (backpressure, retry, offset commits, DLQ) typically don't need a real format
/// pipeline — they just need a `byte[] -> byte[]` operation with optional side effects.
///
/// These helpers build through [MessagePipeline#from] and [MessageFormat#bytes], the path
/// production code takes, rather than a parallel implementation.
///
/// They stay in the test source tree on purpose: they are NOT a public API.
/// Production users should call `MessagePipeline.from(MessageFormat.bytes())` directly.
final class TestPipelines {

  private TestPipelines() {}

  /// A pipeline whose `process()` step runs the given operator.
  ///
  /// @param processor the byte[] transform invoked from `process()`
  /// @return a MessagePipeline driving the given processor
  static MessagePipeline<byte[]> sideEffect(final UnaryOperator<byte[]> processor) {
    return MessagePipeline.from(MessageFormat.bytes()).pipe(processor).build();
  }

  /// An identity pipeline — bytes in, same bytes out, no side effects.
  ///
  /// @return a passthrough MessagePipeline
  static MessagePipeline<byte[]> identity() {
    return MessagePipeline.from(MessageFormat.bytes()).build();
  }
}
