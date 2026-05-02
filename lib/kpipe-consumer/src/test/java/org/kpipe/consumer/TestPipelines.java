package org.kpipe.consumer;

import java.util.function.UnaryOperator;
import org.kpipe.registry.MessagePipeline;

/// Test-only fixtures for [MessagePipeline]. Tests that exercise consumer mechanics
/// (backpressure, retry, offset commits, DLQ) typically don't need a real format
/// pipeline — they just need a `byte[] -> byte[]` operation with optional side effects.
///
/// These helpers stay in the test source tree on purpose: they are NOT a public API.
/// Production users should build pipelines via `MessageProcessorRegistry.pipeline(format)`.
final class TestPipelines {

  private TestPipelines() {}

  /// A pipeline whose `process()` step runs the given operator. `deserialize` and
  /// `serialize` are identity passthroughs. No sink.
  ///
  /// @param processor the byte[] transform invoked from `process()`
  /// @return a MessagePipeline driving the given processor
  static MessagePipeline<byte[]> sideEffect(final UnaryOperator<byte[]> processor) {
    return new MessagePipeline<>() {
      @Override
      public byte[] deserialize(final byte[] data) {
        return data;
      }

      @Override
      public byte[] serialize(final byte[] data) {
        return data;
      }

      @Override
      public byte[] process(final byte[] data) {
        return processor.apply(data);
      }
    };
  }

  /// An identity pipeline — bytes in, same bytes out, no side effects.
  ///
  /// @return a passthrough MessagePipeline
  static MessagePipeline<byte[]> identity() {
    return sideEffect(b -> b);
  }
}
