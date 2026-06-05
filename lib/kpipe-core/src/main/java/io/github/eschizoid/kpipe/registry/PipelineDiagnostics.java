package io.github.eschizoid.kpipe.registry;

/// Builds diagnostic messages for deserialization failures at the byte boundary.
///
/// The §12 burn (protobuf seed messages failing decode because `skipBytes(5)` was wrong for the
/// proto wire envelope) cost real debugging hours. The signal was indirect — `messagesProcessed`
/// rising while `sinkInvocationCount` stayed at 0 — because the exception message gave no hint
/// that the bytes looked like a Confluent envelope. This helper closes that gap: on a failing
/// deserialize it hex-dumps the leading bytes and, when the payload looks like a Confluent
/// magic-byte envelope and the caller forgot to set `skipBytes(...)`, suggests the right count
/// for the configured format.
final class PipelineDiagnostics {

  private static final int HEX_DUMP_BYTES = 16;
  private static final byte CONFLUENT_MAGIC = 0x00;

  private PipelineDiagnostics() {}

  /// Wraps a deserialization failure with a hex dump of the leading bytes and (when applicable)
  /// a hint about a missing `skipBytes(...)` call.
  ///
  /// @param data              the bytes handed to `format.deserialize(...)` after any configured
  ///                          `skipBytes` was applied (may be the original record when
  ///                          `skipBytes == 0`)
  /// @param configuredSkip    the `skipBytes` value the pipeline was built with — only the
  ///                          zero case triggers a "you may need to skip the envelope" hint
  /// @param formatClassName   the simple name of the `MessageFormat` impl, used to pick the
  ///                          right skipBytes suggestion (Avro = 5, Protobuf = 6)
  /// @param cause             the exception thrown by the format
  /// @return a `RuntimeException` whose message carries the diagnostic; the original cause is
  ///         preserved via `getCause()`
  static RuntimeException wrap(
    final byte[] data,
    final int configuredSkip,
    final String formatClassName,
    final RuntimeException cause
  ) {
    final var msg = new StringBuilder("Deserialization failed");
    if (formatClassName != null) msg.append(" in ").append(formatClassName);
    msg.append(": ").append(cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage());
    if (data != null && data.length > 0) {
      msg.append(". Leading bytes: ").append(hexDump(data));
      if (configuredSkip == 0 && data[0] == CONFLUENT_MAGIC) {
        final var suggestion = suggestionFor(formatClassName);
        if (suggestion != null) {
          msg
            .append(". First byte is 0x00 — looks like a Confluent Schema Registry envelope. ")
            .append("Did you forget ")
            .append(suggestion)
            .append("?");
        }
      }
    } else if (data == null) {
      msg.append(". Payload: null");
    } else {
      msg.append(". Payload: empty");
    }
    return new RuntimeException(msg.toString(), cause);
  }

  private static String suggestionFor(final String formatClassName) {
    if (formatClassName == null) return null;
    final var lower = formatClassName.toLowerCase();
    if (lower.contains("avro")) return "`.skipBytes(5)` for the 1-byte magic + 4-byte schema ID";
    if (lower.contains("protobuf") || lower.contains("proto")) {
      return "`.skipBytes(6)` for the 1-byte magic + 4-byte schema ID + 1-byte message-index header (single-message variant)";
    }
    return null;
  }

  private static String hexDump(final byte[] data) {
    final var n = Math.min(data.length, HEX_DUMP_BYTES);
    final var sb = new StringBuilder(2 + n * 3);
    sb.append('[');
    for (int i = 0; i < n; i++) {
      if (i > 0) sb.append(' ');
      sb.append(String.format("%02x", data[i] & 0xff));
    }
    if (data.length > n) sb.append(" …(").append(data.length).append(" bytes total)");
    sb.append(']');
    return sb.toString();
  }
}
