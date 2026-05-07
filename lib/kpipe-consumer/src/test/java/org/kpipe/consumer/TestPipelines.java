package org.kpipe.consumer;

import java.util.function.UnaryOperator;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.MessagePipeline;
import org.kpipe.registry.MessageProcessorRegistry;

/// Test-only fixtures for [MessagePipeline]. Tests that exercise consumer mechanics
/// (backpressure, retry, offset commits, DLQ) typically don't need a real format
/// pipeline — they just need a `byte[] -> byte[]` operation with optional side effects.
///
/// These helpers route through the canonical [MessageProcessorRegistry] +
/// [MessageFormat#bytes] path so tests exercise the real production code path
/// (TypedPipelineBuilder, RegistryEntry, etc.) rather than a parallel implementation.
///
/// They stay in the test source tree on purpose: they are NOT a public API.
/// Production users should call `registry.pipeline(MessageFormat.bytes())` directly.
final class TestPipelines {

  private static final MessageProcessorRegistry REGISTRY = new MessageProcessorRegistry();

  private TestPipelines() {}

  /// A pipeline whose `process()` step runs the given operator.
  ///
  /// @param processor the byte[] transform invoked from `process()`
  /// @return a MessagePipeline driving the given processor
  static MessagePipeline<byte[]> sideEffect(final UnaryOperator<byte[]> processor) {
    return REGISTRY.pipeline(MessageFormat.bytes()).add(processor).build();
  }

  /// An identity pipeline — bytes in, same bytes out, no side effects.
  ///
  /// @return a passthrough MessagePipeline
  static MessagePipeline<byte[]> identity() {
    return REGISTRY.pipeline(MessageFormat.bytes()).build();
  }
}
