package io.github.eschizoid.kpipe.pipeline;

/// The Confluent Schema Registry wire-envelope prefix shared by the Avro and Protobuf
/// registry-mode formats: a 1-byte magic (`0x00`) followed by a 4-byte big-endian schema id. The
/// format-specific remainder, the Avro payload or the Protobuf message-index array, begins at
/// [#HEADER_LENGTH].
///
/// Both formats read the prefix here, so they cannot drift on the envelope contract, and both
/// render the leading bytes via [WireDiagnostics] on a bad envelope.
public final class ConfluentEnvelope {

  private static final byte MAGIC = 0x00;

  /// Bytes consumed by the magic byte and the 4-byte schema id. The format-specific payload or
  /// message-index array starts at this offset.
  public static final int HEADER_LENGTH = 5;

  private ConfluentEnvelope() {}

  /// Validates the magic byte and reads the 4-byte big-endian schema id. The caller's payload or
  /// message-index array begins at [#HEADER_LENGTH]; formats that need at least one further byte
  /// (e.g. Protobuf's message-index) should length-check that separately before calling.
  ///
  /// @param data the full record bytes (non-null; callers null-check the record value first)
  /// @return the schema id encoded in the envelope
  /// @throws MalformedRecordException if the record is shorter than [#HEADER_LENGTH], the magic
  ///     byte is not `0x00`, or the schema id is negative
  public static int readSchemaId(final byte[] data) {
    if (data.length < HEADER_LENGTH) throw new MalformedRecordException(
      "Record too short for Confluent wire envelope: " +
        data.length +
        " bytes; expected at least " +
        HEADER_LENGTH +
        " (first bytes " +
        WireDiagnostics.hexPreview(data) +
        ")"
    );
    if (data[0] != MAGIC) throw new MalformedRecordException(
      "Unexpected magic byte 0x%02x; expected 0x00 (Confluent Schema Registry envelope; first bytes %s)".formatted(
        data[0] & 0xff,
        WireDiagnostics.hexPreview(data)
      )
    );
    final var schemaId =
      ((data[1] & 0xff) << 24) | ((data[2] & 0xff) << 16) | ((data[3] & 0xff) << 8) | (data[4] & 0xff);
    if (schemaId < 0) throw new MalformedRecordException(
      "Negative schema id %d in Confluent wire envelope; schema ids are never negative (first bytes %s)".formatted(
        schemaId,
        WireDiagnostics.hexPreview(data)
      )
    );
    return schemaId;
  }
}
