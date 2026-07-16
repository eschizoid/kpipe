package io.github.eschizoid.kpipe.format.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/// Payload-shape edge cases for [JsonFormat]: oversized documents, truncated UTF-8 input, and
/// non-object top-level JSON. The format must round-trip the large case and fail
/// loudly on the malformed cases — never quietly return a partial map.
class JsonFormatEdgeCasesTest {

  @Test
  void oversizedDocumentRoundTrips() {
    final var big = "y".repeat(2_000_000);
    final var bytes = JsonFormat.INSTANCE.serialize(java.util.Map.of("blob", big));

    final var decoded = JsonFormat.INSTANCE.deserialize(bytes);

    assertNotNull(decoded);
    assertEquals(big, decoded.get("blob"));
  }

  @Test
  void truncatedMidValueThrows() {
    // Valid prefix, cut off inside a string value — a partial parse must not be returned.
    final var truncated = "{\"name\":\"Alic".getBytes(StandardCharsets.UTF_8);
    assertDecodeFailure(truncated);
  }

  @Test
  void invalidUtf8InStringValueThrows() {
    // A lone leading byte of a 3-byte UTF-8 sequence (E2 = first byte of €) inside a string
    // value. The byte[] (UTF-8) decode path validates the encoding and throws rather than
    // returning a half-decoded map that downstream would treat as a processed record.
    final var bytes = new byte[] { '{', '"', 'k', '"', ':', '"', (byte) 0xE2, '"', '}' };
    assertDecodeFailure(bytes);
  }

  @Test
  void topLevelArrayThrowsRatherThanReturningNull() {
    // parseObject expects an object; a top-level array is not a Map<String, Object>. This must
    // throw, never surface as a quietly null or empty map the pipeline would mistake for a filter.
    final var array = "[1,2,3]".getBytes(StandardCharsets.UTF_8);
    assertDecodeFailure(array);
  }

  @Test
  void topLevelScalarThrowsRatherThanReturningNull() {
    final var scalar = "42".getBytes(StandardCharsets.UTF_8);
    assertDecodeFailure(scalar);
  }

  @Test
  void unbalancedBracesThrow() {
    final var unbalanced = "{\"a\":{\"b\":1}".getBytes(StandardCharsets.UTF_8);
    assertDecodeFailure(unbalanced);
  }

  /// Asserts the input fails as the format's own decode error — not a generic exception that would
  /// green this test on an unrelated bug. `JsonFormat.deserialize` wraps parse failures with this
  /// prefix; anything else (NPE, coercion) propagates without it and fails the check loudly.
  private static void assertDecodeFailure(final byte[] data) {
    final var ex = assertThrows(IllegalStateException.class, () -> JsonFormat.INSTANCE.deserialize(data));
    assertTrue(
      ex.getMessage() != null && ex.getMessage().contains("JsonFormat.deserialize failed"),
      () -> "expected a JsonFormat decode failure, got: " + ex
    );
  }
}
