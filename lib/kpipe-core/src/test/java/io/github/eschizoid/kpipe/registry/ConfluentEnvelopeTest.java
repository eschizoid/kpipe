package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/// Unit tests for [ConfluentEnvelope] — the shared magic-byte check + big-endian schema-id read that
/// the Avro and Protobuf registry-mode formats delegate to.
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
  void treatsTheHighBitOfEachIdByteAsUnsigned() {
    // 0xFFFFFFFF must read back as -1, not sign-extend wrong through the shifts.
    final var data = new byte[] { 0x00, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
    assertEquals(-1, ConfluentEnvelope.readSchemaId(data));
  }

  @Test
  void rejectsAWrongMagicByte() {
    final var data = new byte[] { 0x01, 0x00, 0x00, 0x00, 0x07 };
    final var ex = assertThrows(IllegalStateException.class, () -> ConfluentEnvelope.readSchemaId(data));
    assertTrue(ex.getMessage().contains("magic byte"), ex.getMessage());
  }

  @Test
  void rejectsARecordShorterThanTheHeader() {
    final var data = new byte[] { 0x00, 0x00, 0x00 }; // 3 bytes < HEADER_LENGTH (5)
    final var ex = assertThrows(IllegalStateException.class, () -> ConfluentEnvelope.readSchemaId(data));
    assertTrue(ex.getMessage().contains("too short"), ex.getMessage());
  }

  @Test
  void headerLengthIsFiveMagicPlusFourByteId() {
    assertEquals(5, ConfluentEnvelope.HEADER_LENGTH);
  }
}
