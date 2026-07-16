package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/// Pins the shared [WireDiagnostics] byte-preview contract that both the pipeline boundary
/// ([PipelineDiagnostics]) and the format modules depend on: lowercase, space-delimited, capped,
/// truncation markers, and never throwing on empty/short input.
class WireDiagnosticsTest {

  @Test
  void hexPreviewEmptyArrayIsEmptyString() {
    assertEquals("", WireDiagnostics.hexPreview(new byte[0]));
  }

  @Test
  void hexPreviewRendersLowercaseSpaceDelimited() {
    assertEquals("00 01 ff", WireDiagnostics.hexPreview(new byte[] {0x00, 0x01, (byte) 0xff}));
  }

  @Test
  void hexPreviewExactlyLimitHasNoEllipsis() {
    final var preview = WireDiagnostics.hexPreview(new byte[WireDiagnostics.HEX_PREVIEW_LIMIT]);
    assertFalse(preview.contains("..."), "no ellipsis when the array is not truncated");
    assertEquals(WireDiagnostics.HEX_PREVIEW_LIMIT, preview.split(" ").length);
  }

  @Test
  void hexPreviewBeyondLimitTruncatesWithEllipsis() {
    final var preview = WireDiagnostics.hexPreview(new byte[WireDiagnostics.HEX_PREVIEW_LIMIT + 4]);
    assertTrue(preview.endsWith(" ..."), "truncated preview ends with an ellipsis; got: " + preview);
    final var rendered = preview.substring(0, preview.length() - " ...".length());
    assertEquals(WireDiagnostics.HEX_PREVIEW_LIMIT, rendered.split(" ").length, "only the first LIMIT bytes rendered");
  }

  @Test
  void hexDumpRendersUpToMaxBytesWithoutSuffixWhenNotTruncated() {
    assertEquals("00 01 ff", WireDiagnostics.hexDump(new byte[] {0x00, 0x01, (byte) 0xff}, 8));
  }

  @Test
  void hexDumpAppendsTotalCountWhenTruncated() {
    assertEquals("00 01 …(4 bytes total)", WireDiagnostics.hexDump(new byte[] {0x00, 0x01, 0x02, 0x03}, 2));
  }
}
