package io.github.eschizoid.kpipe.diagnostics;

import java.util.HexFormat;

/// Cross-format diagnostic helpers for wire-level failures. Shared by the format modules so a
/// malformed-payload error message carries the same byte preview everywhere (JSON, Avro, Protobuf)
/// instead of each format hand-rolling — and drifting on — its own copy.
public final class Diagnostics {

  /// Maximum number of leading bytes rendered by [#hexPreview].
  public static final int HEX_PREVIEW_LIMIT = 8;

  private Diagnostics() {}

  /// Renders a short hex preview of the leading bytes of `data` for diagnostics, capped at
  /// [#HEX_PREVIEW_LIMIT] bytes. Handles empty/short arrays without throwing — the single most
  /// useful field in a wire-format-mismatch error (wrong magic byte, wrong `skipBytes`, writer/reader
  /// schema mismatch on real bytes).
  ///
  /// @param data the byte array to preview (must be non-null)
  /// @return space-separated lowercase hex of the leading bytes, with a trailing ` ...` when truncated
  public static String hexPreview(final byte[] data) {
    final var count = Math.min(data.length, HEX_PREVIEW_LIMIT);
    final var hex = HexFormat.ofDelimiter(" ").formatHex(data, 0, count);
    return count < data.length ? hex + " ..." : hex;
  }
}
