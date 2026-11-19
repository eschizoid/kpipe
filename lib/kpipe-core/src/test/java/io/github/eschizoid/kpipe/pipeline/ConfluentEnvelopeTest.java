package io.github.eschizoid.kpipe.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/// Unit tests for [ConfluentEnvelope]: the shared magic-byte check and big-endian schema-id read
/// that the Avro and Protobuf registry-mode formats delegate to.
class ConfluentEnvelopeTest {

  @Test
  void readsTheBigEndianSchemaIdAfterTheMagicByte() {
    // magic 0x00, then schema id 7 as 4 big-endian bytes, then one payload byte.
    final var data = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x07, 0x2a };
    assertEquals(7, ConfluentEnvelope.readSchemaId(data));
  }

  @Test
  void readsAMultiByteSchemaId() {
    // 0x01020304 = 16909060.
    final var data = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x00 };
    assertEquals(0x01020304, ConfluentEnvelope.readSchemaId(data));
  }

  @Test
  void treatsTheHighBitOfTheLowIdBytesAsUnsigned() {
    // 0x7FFFFFFF: every byte but the first has its high bit set, and none may sign-extend.
    final var data = new byte[] { 0x00, 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff };
    assertEquals(Integer.MAX_VALUE, ConfluentEnvelope.readSchemaId(data));
  }

  /// A negative id is decided by the bytes alone, so it is malformed rather than a registry lookup
  /// that a retry might fix.
  @Test
  void rejectsANegativeSchemaId() {
    for (final var data : new byte[][] {
      { 0x00, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff },
      { 0x00, (byte) 0x80, 0x00, 0x00, 0x01 },
    }) {
      final var ex = assertThrows(MalformedRecordException.class, () -> ConfluentEnvelope.readSchemaId(data));
      assertTrue(ex.getMessage().contains("Negative schema id"), ex.getMessage());
    }
  }

  @Test
  void rejectsAWrongMagicByte() {
    final var data = new byte[] { 0x01, 0x00, 0x00, 0x00, 0x07 };
    final var ex = assertThrows(MalformedRecordException.class, () -> ConfluentEnvelope.readSchemaId(data));
    assertTrue(ex.getMessage().contains("magic byte"), ex.getMessage());
  }

  @Test
  void rejectsARecordShorterThanTheHeader() {
    final var data = new byte[] { 0x00, 0x00, 0x00 }; // 3 bytes < HEADER_LENGTH (5)
    final var ex = assertThrows(MalformedRecordException.class, () -> ConfluentEnvelope.readSchemaId(data));
    assertTrue(ex.getMessage().contains("too short"), ex.getMessage());
  }

  @Test
  void headerLengthIsFiveMagicPlusFourByteId() {
    assertEquals(5, ConfluentEnvelope.HEADER_LENGTH);
  }
}
