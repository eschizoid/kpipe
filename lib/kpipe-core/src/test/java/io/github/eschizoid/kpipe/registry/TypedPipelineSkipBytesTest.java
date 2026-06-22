package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

/// Pins the wire-prefix stripping done by the pipeline built from [TypedPipelineBuilder].
///
/// `skipBytes(n)` is the seam that strips Confluent magic-byte envelopes (5 bytes for Avro,
/// 6 for single-message Protobuf) before the format ever sees the payload. An off-by-one here
/// is the classic silent-corruption bug: skip one byte too few and the format decodes the
/// stray prefix byte as payload; skip one too many and the first real byte is lost. These
/// tests verify the slice handed to the format is exactly `data[n..]`, and that a payload no
/// longer than the prefix surfaces through the no-null contract rather than slipping through
/// as a quietly filtered record.
class TypedPipelineSkipBytesTest {

  /// Records the exact bytes the format was asked to deserialize so the test can assert on the
  /// post-skip slice. Echoes the bytes back as the "decoded" value.
  private static final class CapturingFormat implements MessageFormat<byte[]> {

    byte[] lastSeen;

    @Override
    public byte[] serialize(final byte[] data) {
      return data;
    }

    @Override
    public byte[] deserialize(final byte[] data) {
      lastSeen = data;
      return data;
    }
  }

  private static MessagePipeline<byte[]> pipelineWithSkip(final MessageFormat<byte[]> format, final int skip) {
    return new TypedPipelineBuilder<>(format, new MessageProcessorRegistry()).skipBytes(skip).build();
  }

  @Test
  void stripsExactlyTheConfiguredPrefixForAvroEnvelope() {
    // 1-byte magic + 4-byte schema id + 3-byte payload.
    final var record = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x42, 0x11, 0x22, 0x33 };
    final var format = new CapturingFormat();

    final var decoded = pipelineWithSkip(format, 5).deserialize(record);

    final var expectedPayload = new byte[] { 0x11, 0x22, 0x33 };
    assertArrayEquals(expectedPayload, format.lastSeen, "format must see only the bytes after the 5-byte envelope");
    assertArrayEquals(expectedPayload, decoded, "the decoded value is the post-skip slice");
  }

  @Test
  void stripsExactlyTheConfiguredPrefixForProtobufEnvelope() {
    // 1-byte magic + 4-byte schema id + 1-byte message-index header + 2-byte payload.
    final var record = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, (byte) 0xAB, (byte) 0xCD };
    final var format = new CapturingFormat();

    pipelineWithSkip(format, 6).deserialize(record);

    assertArrayEquals(
      new byte[] { (byte) 0xAB, (byte) 0xCD },
      format.lastSeen,
      "format must see only the bytes after the 6-byte envelope"
    );
  }

  @Test
  void offByOneSkipLeaksOrDropsAByteRatherThanMatching() {
    // Same Avro-shaped record; only skip(5) yields the clean payload. skip(4) leaks the last
    // envelope byte; skip(6) eats the first payload byte. Both are silent corruption, so pin
    // the boundary so a regression that changes the slice length is caught.
    final var record = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x42, 0x11, 0x22, 0x33 };
    final var clean = new byte[] { 0x11, 0x22, 0x33 };

    final var four = new CapturingFormat();
    pipelineWithSkip(four, 4).deserialize(record);
    assertArrayEquals(new byte[] { 0x42, 0x11, 0x22, 0x33 }, four.lastSeen, "skip(4) leaks the trailing envelope byte");
    assertNotEquals(Arrays.toString(clean), Arrays.toString(four.lastSeen));

    final var six = new CapturingFormat();
    pipelineWithSkip(six, 6).deserialize(record);
    assertArrayEquals(new byte[] { 0x22, 0x33 }, six.lastSeen, "skip(6) drops the first payload byte");
    assertNotEquals(Arrays.toString(clean), Arrays.toString(six.lastSeen));
  }

  @Test
  void payloadShorterThanPrefixDeserializesToNull() {
    final var format = new CapturingFormat();
    // 4 bytes of envelope, none of payload, against a 5-byte skip.
    final var tooShort = new byte[] { 0x00, 0x00, 0x00, 0x00 };

    assertNull(pipelineWithSkip(format, 5).deserialize(tooShort), "no payload after the prefix decodes to null");
    assertNull(format.lastSeen, "the format must never be invoked when there is nothing left to decode");
  }

  @Test
  void payloadExactlyEqualToPrefixLengthDeserializesToNull() {
    final var format = new CapturingFormat();
    // Exactly the 5-byte envelope, zero payload. The boundary is `data.length <= skip`.
    final var envelopeOnly = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x42 };

    assertNull(pipelineWithSkip(format, 5).deserialize(envelopeOnly), "an envelope with no payload decodes to null");
    assertNull(format.lastSeen);
  }

  @Test
  void deserializeOrFailRejectsTheTruncatedPrefixAsAContractViolation() {
    // The honest end-to-end path: a too-short record must not be reported as a silently
    // filtered record. deserializeOrFail turns the null into the retryable no-null violation.
    final var pipeline = pipelineWithSkip(new CapturingFormat(), 5);
    final var tooShort = new byte[] { 0x00, 0x00, 0x00 };

    final var ex = assertThrows(IllegalStateException.class, () -> pipeline.deserializeOrFail(tooShort));
    assertEquals("deserialize() returned null — implementations must throw on malformed input", ex.getMessage());
  }

  @Test
  void singleTrailingByteAfterPrefixIsKept() {
    final var format = new CapturingFormat();
    // One byte more than the prefix — the smallest non-null slice.
    final var record = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x42, 0x7e };

    pipelineWithSkip(format, 5).deserialize(record);

    assertArrayEquals(new byte[] { 0x7e }, format.lastSeen);
  }

  @Test
  void zeroSkipHandsTheWholeRecordToTheFormat() {
    final var format = new CapturingFormat();
    final var record = new byte[] { 0x11, 0x22, 0x33 };

    pipelineWithSkip(format, 0).deserialize(record);

    assertArrayEquals(record, format.lastSeen, "skip(0) is a passthrough — no slicing");
  }
}
