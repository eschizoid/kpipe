package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  /// A format whose decode always fails, as a registry-backed one or not. Its name contains "Avro",
  /// which is what picks the `skipBytes(5)` hint.
  private record FailingAvroFormat(boolean registryBacked, RuntimeException failure) implements MessageFormat<byte[]> {
    @Override
    public byte[] serialize(final byte[] data) {
      return data;
    }

    @Override
    public byte[] deserialize(final byte[] data) {
      throw failure;
    }

    @Override
    public boolean isSchemaRegistryBacked() {
      return registryBacked;
    }
  }

  private static final class NullFormat implements MessageFormat<byte[]> {

    @Override
    public byte[] serialize(final byte[] data) {
      return data;
    }

    @Override
    public byte[] deserialize(final byte[] data) {
      return null;
    }
  }

  private static MessagePipeline<byte[]> pipelineWithSkip(final MessageFormat<byte[]> format, final int skip) {
    return MessagePipeline.from(format).skipBytes(skip).build();
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
  void payloadShorterThanPrefixIsMalformed() {
    final var format = new CapturingFormat();
    // 4 bytes of envelope, none of payload, against a 5-byte skip.
    final var tooShort = new byte[] { 0x00, 0x00, 0x00, 0x00 };

    final var ex = assertThrows(MalformedRecordException.class, () ->
      pipelineWithSkip(format, 5).deserialize(tooShort)
    );
    assertEquals("The record value is 4 bytes, so skipBytes(5) leaves nothing to deserialize", ex.getMessage());
    assertNull(format.lastSeen, "the format must never be invoked when there is nothing left to decode");
  }

  @Test
  void payloadExactlyEqualToPrefixLengthIsMalformed() {
    final var format = new CapturingFormat();
    // Exactly the 5-byte envelope, zero payload. The boundary is `data.length <= skip`.
    final var envelopeOnly = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x42 };

    assertThrows(MalformedRecordException.class, () -> pipelineWithSkip(format, 5).deserialize(envelopeOnly));
    assertNull(format.lastSeen);
  }

  @Test
  void aNullValueIsMalformed() {
    final var tombstone = assertThrows(MalformedRecordException.class, () ->
      pipelineWithSkip(new CapturingFormat(), 0).deserialize(null)
    );
    assertTrue(tombstone.getMessage().contains("tombstone"), tombstone.getMessage());
  }

  /// An empty value is the format's call: raw bytes pass it on, a decoding format returns null.
  @Test
  void anEmptyValueIsUpToTheFormat() {
    assertArrayEquals(new byte[0], pipelineWithSkip(MessageFormat.bytes(), 0).deserialize(new byte[0]));

    final var empty = assertThrows(MalformedRecordException.class, () ->
      pipelineWithSkip(new NullFormat(), 0).deserialize(new byte[0])
    );
    assertEquals("The record value is empty, so there is nothing to deserialize", empty.getMessage());
  }

  /// A decode failure keeps its type through the pipeline, with or without a skipped prefix, so
  /// bad bytes stay non-retryable and anything else stays retryable.
  @Test
  void aDecodeFailureKeepsItsRetryability() {
    final var envelope = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x01, 0x02 };
    for (final var skip : new int[] { 0, 1 }) {
      final var bad = new FailingAvroFormat(false, new MalformedRecordException("bad"));
      assertThrows(MalformedRecordException.class, () -> pipelineWithSkip(bad, skip).deserialize(envelope));
      final var down = new FailingAvroFormat(false, new IllegalStateException("down"));
      final var ex = assertThrows(RuntimeException.class, () -> pipelineWithSkip(down, skip).deserialize(envelope));
      assertFalse(NonRetryableException.isNonRetryable(ex), "skip " + skip + ": " + ex);
    }
  }

  /// Only a format that does not read the envelope itself gets the `skipBytes` hint.
  @Test
  void theSkipBytesHintFollowsTheFormat() {
    final var envelope = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x01, 0x02 };
    final var plain = new FailingAvroFormat(false, new IllegalStateException("bad"));
    final var registry = new FailingAvroFormat(true, new IllegalStateException("bad"));
    final var plainEx = assertThrows(RuntimeException.class, () -> pipelineWithSkip(plain, 0).deserialize(envelope));
    final var registryEx = assertThrows(RuntimeException.class, () ->
      pipelineWithSkip(registry, 0).deserialize(envelope)
    );
    assertTrue(plainEx.getMessage().contains("Confluent Schema Registry envelope"), plainEx.getMessage());
    assertFalse(registryEx.getMessage().contains("skipBytes"), registryEx.getMessage());
  }

  /// A built pipeline never returns null, whether or not it skips a prefix.
  @Test
  void aFormatReturningNullIsMalformed() {
    final var plain = assertThrows(MalformedRecordException.class, () ->
      pipelineWithSkip(new NullFormat(), 0).deserialize(new byte[] { 1, 2, 3 })
    );
    assertEquals("NullFormat returned null for a 3-byte value instead of throwing", plain.getMessage());
    final var skipped = assertThrows(MalformedRecordException.class, () ->
      pipelineWithSkip(new NullFormat(), 1).deserialize(new byte[] { 1, 2, 3 })
    );
    assertEquals("NullFormat returned null for a 3-byte value instead of throwing", skipped.getMessage());
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
