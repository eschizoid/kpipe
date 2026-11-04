package io.github.eschizoid.kpipe.format.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.WireFormat;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// Operators that edit encoded Protobuf messages by field number, without decoding them, for a
/// `KPipe.bytes(...)` stream or a bytes pipeline. Each one copies the fields it keeps byte for
/// byte and skips the rest, so a record that is forwarded to Kafka is never decoded into a
/// message object or encoded again.
///
/// ```java
/// import static io.github.eschizoid.kpipe.format.protobuf.ProtobufBytes.*;
///
/// KPipe.bytes("users", props)
///     .filter(has(EMAIL))
///     .pipe(remove(SSN))
///     .pipe(setString(STATUS, "processed"))
///     .toCustom(kafkaSink)
///     .start();
/// ```
///
/// The operators know field numbers and wire types, not the schema. The caller picks the setter
/// that matches the field's declared type: [#setLong] writes a varint, which is right for `int32`,
/// `int64`, `uint32`, `uint64`, `bool` and enums but not for `sint32`/`sint64` or the fixed-width
/// types. A setter that finds the field already written with another wire type fails the record.
///
/// A setter removes every occurrence of the field and appends one. Parsers keep the last value of
/// a singular scalar field, so the result reads as the new value; a repeated field ends up with
/// the one value. Field order changes, which Protobuf allows.
///
/// The bytes must be a bare Protobuf message, with no Confluent wire envelope in front. Bytes
/// whose top-level fields are malformed throw [IllegalArgumentException], which fails the record.
/// The contents of length-delimited fields are copied unchecked, and [#has] stops reading at the
/// first match.
public final class ProtobufBytes {

  private static final byte[] EMPTY = new byte[0];

  /// A tag holds the field number in 29 bits.
  private static final int MAX_FIELD_NUMBER = (1 << 29) - 1;

  private ProtobufBytes() {}

  /// Removes every occurrence of each of `fieldNumbers`, including packed repeated values.
  ///
  /// @param fieldNumbers the fields to remove, at least one
  /// @return an operator that removes the fields
  /// @throws IllegalArgumentException if no field number is given or one is not positive
  public static UnaryOperator<byte[]> remove(final int... fieldNumbers) {
    final var removed = fieldSet("remove", fieldNumbers);
    return message -> rewrite(message, field -> !removed.test(field), null);
  }

  /// Removes every field except `fieldNumbers`.
  ///
  /// @param fieldNumbers the fields to keep, at least one
  /// @return an operator that keeps only the fields
  /// @throws IllegalArgumentException if no field number is given or one is not positive
  public static UnaryOperator<byte[]> keep(final int... fieldNumbers) {
    final var kept = fieldSet("keep", fieldNumbers);
    return message -> rewrite(message, kept, null);
  }

  /// Sets a `string` field.
  ///
  /// @param fieldNumber the field
  /// @param value the value
  /// @return an operator that sets the field
  public static UnaryOperator<byte[]> setString(final int fieldNumber, final String value) {
    Objects.requireNonNull(value, "value");
    return set(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED, out -> out.writeString(fieldNumber, value));
  }

  /// Sets a `bytes` field, or an embedded message field to an encoded message.
  ///
  /// @param fieldNumber the field
  /// @param value the value
  /// @return an operator that sets the field
  public static UnaryOperator<byte[]> setBytes(final int fieldNumber, final byte[] value) {
    final var copy = ByteString.copyFrom(Objects.requireNonNull(value, "value"));
    return set(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED, out -> out.writeBytes(fieldNumber, copy));
  }

  /// Sets a varint field: `int32`, `int64`, `uint32`, `uint64`, `bool` (0 or 1) or an enum. The
  /// value is written as a 64-bit varint; a parser of a 32-bit or enum field keeps the low 32 bits.
  ///
  /// @param fieldNumber the field
  /// @param value the value
  /// @return an operator that sets the field
  public static UnaryOperator<byte[]> setLong(final int fieldNumber, final long value) {
    return set(fieldNumber, WireFormat.WIRETYPE_VARINT, out -> out.writeInt64(fieldNumber, value));
  }

  /// Matches messages that contain `fieldNumber` at least once on the wire. A singular scalar field
  /// set to its default value is usually not written, so it does not match. It reports what is
  /// written, not what a parser keeps: a `oneof` member followed by another member still matches,
  /// although a parser clears it.
  ///
  /// @param fieldNumber the field
  /// @return a predicate for `filter`
  public static Predicate<byte[]> has(final int fieldNumber) {
    checkFieldNumber(fieldNumber);
    return message -> {
      final var in = CodedInputStream.newInstance(message);
      try {
        int tag;
        while ((tag = in.readTag()) != 0) {
          if (WireFormat.getTagFieldNumber(tag) == fieldNumber) return true;
          in.skipField(tag);
        }
        return false;
      } catch (final IOException e) {
        throw malformed(e);
      }
    };
  }

  private interface FieldWriter {
    void write(CodedOutputStream out) throws IOException;
  }

  private static UnaryOperator<byte[]> set(final int fieldNumber, final int wireType, final FieldWriter writer) {
    checkFieldNumber(fieldNumber);
    final var append = new Append(fieldNumber, wireType, encode(writer));
    return message -> rewrite(message, field -> field != fieldNumber, append);
  }

  /// A field to write after the kept ones, encoded once when the operator is created.
  private record Append(int fieldNumber, int wireType, byte[] encoded) {}

  /// Copies the fields `keep` accepts, then writes `append` if there is one. Two passes over the
  /// input: the first finds the kept ranges and their total size, so the output is allocated once
  /// at its exact length.
  private static byte[] rewrite(final byte[] message, final IntPredicate keep, final Append append) {
    final var in = CodedInputStream.newInstance(message);
    var ranges = new int[16];
    var count = 0;
    var keptBytes = 0;
    try {
      while (true) {
        final var start = in.getTotalBytesRead();
        final var tag = in.readTag();
        if (tag == 0) break;
        final var field = WireFormat.getTagFieldNumber(tag);
        if (append != null && field == append.fieldNumber() && WireFormat.getTagWireType(tag) != append.wireType()) {
          throw new IllegalArgumentException(
            "Field %d has wire type %d, not %d".formatted(field, WireFormat.getTagWireType(tag), append.wireType())
          );
        }
        in.skipField(tag);
        if (!keep.test(field)) continue;
        final var end = in.getTotalBytesRead();
        if (count * 2 == ranges.length) ranges = Arrays.copyOf(ranges, ranges.length * 2);
        if (count > 0 && ranges[count * 2 - 1] == start) {
          ranges[count * 2 - 1] = end;
        } else {
          ranges[count * 2] = start;
          ranges[count * 2 + 1] = end;
          count++;
        }
        keptBytes += end - start;
      }
    } catch (final IOException e) {
      throw malformed(e);
    }
    if (append == null && keptBytes == message.length) return message;

    final var appended = append == null ? EMPTY : append.encoded();
    final var out = new byte[keptBytes + appended.length];
    var position = 0;
    for (var i = 0; i < count; i++) {
      final var length = ranges[i * 2 + 1] - ranges[i * 2];
      System.arraycopy(message, ranges[i * 2], out, position, length);
      position += length;
    }
    System.arraycopy(appended, 0, out, position, appended.length);
    return out;
  }

  private static byte[] encode(final FieldWriter writer) {
    final var buffer = new java.io.ByteArrayOutputStream();
    final var out = CodedOutputStream.newInstance(buffer);
    try {
      writer.write(out);
      out.flush();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return buffer.toByteArray();
  }

  private static IntPredicate fieldSet(final String operator, final int... fieldNumbers) {
    Objects.requireNonNull(fieldNumbers, "fieldNumbers");
    if (fieldNumbers.length == 0) throw new IllegalArgumentException(
      "%s needs at least one field number".formatted(operator)
    );
    for (final var field : fieldNumbers) checkFieldNumber(field);
    final var sorted = fieldNumbers.clone();
    Arrays.sort(sorted);
    return field -> Arrays.binarySearch(sorted, field) >= 0;
  }

  private static void checkFieldNumber(final int fieldNumber) {
    if (fieldNumber < 1 || fieldNumber > MAX_FIELD_NUMBER) throw new IllegalArgumentException(
      "Field numbers are 1 to %d, got %d".formatted(MAX_FIELD_NUMBER, fieldNumber)
    );
  }

  private static IllegalArgumentException malformed(final IOException e) {
    return new IllegalArgumentException(
      e instanceof InvalidProtocolBufferException ? "Malformed Protobuf message: " + e.getMessage() : e.getMessage(),
      e
    );
  }
}
