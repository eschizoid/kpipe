package io.github.eschizoid.kpipe.registry;

import java.util.HexFormat;

/// Cross-cutting helpers for rendering leading bytes in wire-level failure messages. One home so the
/// pipeline boundary ([PipelineDiagnostics]) and the format modules (JSON/Avro/Protobuf) render byte
/// previews the same way instead of each hand-rolling — and drifting on — its own copy.
///
/// Two presentations, deliberately distinct:
///   * [#hexPreview] — compact, up to [#HEX_PREVIEW_LIMIT] bytes, ` ...` when truncated. For
///     embedding inside a format's own error message where brevity matters.
///   * [#hexDump] — fuller, a caller-chosen byte cap plus the total length when truncated. For a
///     top-level deserialization-failure message where the extra context earns its space.
///
/// Both are lowercase, space-delimited, and never throw on empty/short input (they run inside error
/// paths, where throwing would mask the original cause). `data` must be non-null.
public final class WireDiagnostics {

  /// Maximum number of leading bytes rendered by [#hexPreview].
  public static final int HEX_PREVIEW_LIMIT = 8;

  private WireDiagnostics() {}

  /// Compact preview of the leading bytes of `data`, capped at [#HEX_PREVIEW_LIMIT] — the single
  /// most useful field in a wire-format-mismatch error (wrong magic byte, wrong `skipBytes`,
  /// writer/reader schema mismatch on real bytes).
  ///
  /// @param data the byte array to preview (must be non-null)
  /// @return space-separated lowercase hex of the leading bytes, with a trailing ` ...` when truncated
  public static String hexPreview(final byte[] data) {
    final var count = Math.min(data.length, HEX_PREVIEW_LIMIT);
    final var hex = HexFormat.ofDelimiter(" ").formatHex(data, 0, count);
    return count < data.length ? hex + " ..." : hex;
  }

  /// Fuller dump of the leading bytes of `data`, capped at `maxBytes`, suffixed with the total
  /// length when truncated so a reader knows how much wasn't shown.
  ///
  /// @param data     the byte array to dump (must be non-null)
  /// @param maxBytes the maximum number of leading bytes to render
  /// @return space-separated lowercase hex, with a `…(N bytes total)` suffix when truncated
  public static String hexDump(final byte[] data, final int maxBytes) {
    final var count = Math.min(data.length, maxBytes);
    final var hex = HexFormat.ofDelimiter(" ").formatHex(data, 0, count);
    return count < data.length ? hex + " …(" + data.length + " bytes total)" : hex;
  }
}
