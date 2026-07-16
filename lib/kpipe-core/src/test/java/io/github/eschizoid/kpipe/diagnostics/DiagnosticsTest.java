package io.github.eschizoid.kpipe.diagnostics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/// Pins the shared [Diagnostics#hexPreview] contract the format modules depend on: lowercase,
/// space-delimited, capped at [Diagnostics#HEX_PREVIEW_LIMIT], ellipsis only when truncated, and
/// never throwing on empty/short input.
class DiagnosticsTest {

  @Test
  void emptyArrayPreviewsAsEmptyString() {
    assertEquals("", Diagnostics.hexPreview(new byte[0]));
  }

  @Test
  void shortArrayRendersAllBytesLowercaseSpaceDelimited() {
    assertEquals("00 01 ff", Diagnostics.hexPreview(new byte[] {0x00, 0x01, (byte) 0xff}));
  }

  @Test
  void exactlyLimitBytesHasNoEllipsis() {
    final var preview = Diagnostics.hexPreview(new byte[Diagnostics.HEX_PREVIEW_LIMIT]);
    assertFalse(preview.contains("..."), "no ellipsis when the array is not truncated");
    assertEquals(Diagnostics.HEX_PREVIEW_LIMIT, preview.split(" ").length);
  }

  @Test
  void moreThanLimitBytesTruncatesWithEllipsis() {
    final var preview = Diagnostics.hexPreview(new byte[Diagnostics.HEX_PREVIEW_LIMIT + 4]);
    assertTrue(preview.endsWith(" ..."), "truncated preview ends with an ellipsis; got: " + preview);
    final var rendered = preview.substring(0, preview.length() - " ...".length());
    assertEquals(Diagnostics.HEX_PREVIEW_LIMIT, rendered.split(" ").length, "only the first LIMIT bytes are rendered");
  }
}
