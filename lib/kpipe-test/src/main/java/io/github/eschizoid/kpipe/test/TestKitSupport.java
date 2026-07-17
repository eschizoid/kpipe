package io.github.eschizoid.kpipe.test;

import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.util.List;
import java.util.Properties;
import java.util.function.UnaryOperator;

/// Shared construction plumbing for the Docker-free test kit.
///
/// Both harnesses build the same two artifacts: a registry-backed [MessagePipeline] over a format,
/// an operator chain, and an optional sink; and a minimal, never-contacted consumer `Properties`
/// object. Holding both here stops [TestStream] and [CrashRestartHarness] from drifting — before
/// this, each hand-rolled an identical builder loop and property set (differing only in group id).
final class TestKitSupport {

  private TestKitSupport() {}

  /// Builds a pipeline over `format`, appending `operators` in order and terminating in `sink`
  /// when one is given (`null` leaves the pipeline sink-less, e.g. for batch routes where the
  /// batch sink is wired on the consumer builder instead).
  ///
  /// @param format the SerDe for the pipeline value type
  /// @param operators the operator chain, applied in list order
  /// @param sink the terminal sink, or `null` for a sink-less pipeline
  /// @param <T> the pipeline value type
  /// @return the built pipeline
  static <T> MessagePipeline<T> pipeline(
    final MessageFormat<T> format,
    final List<UnaryOperator<T>> operators,
    final MessageSink<T> sink
  ) {
    final var builder = new MessageProcessorRegistry().pipeline(format);
    for (final var op : operators) builder.add(op);
    if (sink != null) builder.toSink(sink);
    return builder.build();
  }

  /// Minimal, never-contacted consumer config — the harnesses' `MockConsumer` supplier bypasses
  /// the real client, so only the properties object's presence matters.
  ///
  /// @param groupId the consumer group id (only distinguishes the harnesses in diagnostics)
  /// @return the consumer properties
  static Properties props(final String groupId) {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", groupId);
    p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("enable.auto.commit", "false");
    return p;
  }
}
