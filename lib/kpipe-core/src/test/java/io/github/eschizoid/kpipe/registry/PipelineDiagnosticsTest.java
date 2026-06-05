package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PipelineDiagnosticsTest {

  @Test
  void suggestsSkipBytes5ForAvroWhenPayloadStartsWithConfluentMagicByte() {
    final var data = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x42, 0x01, 0x02, 0x03 };
    final var cause = new RuntimeException("Malformed data");

    final var wrapped = PipelineDiagnostics.wrap(data, 0, "AvroFormat", cause);

    assertTrue(wrapped.getMessage().contains("Confluent Schema Registry envelope"));
    assertTrue(wrapped.getMessage().contains("`.skipBytes(5)`"));
    assertSame(cause, wrapped.getCause());
  }

  @Test
  void suggestsSkipBytes6ForProtobufWhenPayloadStartsWithConfluentMagicByte() {
    final var data = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x42, 0x00 };
    final var wrapped = PipelineDiagnostics.wrap(data, 0, "ProtobufFormat", new RuntimeException("bad tag"));

    assertTrue(wrapped.getMessage().contains("`.skipBytes(6)`"));
  }

  @Test
  void doesNotSuggestSkipBytesWhenSkipAlreadyConfigured() {
    final var data = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x42, 0x01 };
    final var wrapped = PipelineDiagnostics.wrap(data, 5, "AvroFormat", new RuntimeException("still wrong"));

    assertTrue(wrapped.getMessage().contains("Deserialization failed"));
    assertTrue(wrapped.getMessage().contains("Leading bytes"));
    assertTrue(!wrapped.getMessage().contains("Confluent Schema Registry envelope"));
  }

  @Test
  void doesNotSuggestSkipBytesWhenFirstByteIsNotMagic() {
    final var data = new byte[] { 0x7b, 0x22, 0x66, 0x6f }; // {"fo
    final var wrapped = PipelineDiagnostics.wrap(data, 0, "JsonFormat", new RuntimeException("not json"));

    assertTrue(!wrapped.getMessage().contains("Confluent Schema Registry envelope"));
    assertTrue(wrapped.getMessage().contains("Leading bytes: [7b 22 66 6f]"));
  }

  @Test
  void doesNotSuggestForUnknownFormatEvenWithMagicByte() {
    final var data = new byte[] { 0x00, 0x01, 0x02 };
    final var wrapped = PipelineDiagnostics.wrap(data, 0, "CustomFormat", new RuntimeException("x"));

    assertTrue(!wrapped.getMessage().contains("skipBytes"));
  }

  @Test
  void truncatesLongPayloadAndReportsTotalLength() {
    final var data = new byte[64];
    for (int i = 0; i < data.length; i++) data[i] = (byte) i;
    final var wrapped = PipelineDiagnostics.wrap(data, 0, "AvroFormat", new RuntimeException("bad"));

    assertTrue(wrapped.getMessage().contains("…(64 bytes total)"));
  }

  @Test
  void handlesEmptyAndNullPayloads() {
    final var emptyWrapped = PipelineDiagnostics.wrap(new byte[0], 0, "AvroFormat", new RuntimeException("e"));
    assertTrue(emptyWrapped.getMessage().contains("Payload: empty"));

    final var nullWrapped = PipelineDiagnostics.wrap(null, 0, "AvroFormat", new RuntimeException("n"));
    assertTrue(nullWrapped.getMessage().contains("Payload: null"));
  }

  @Test
  void preservesOriginalCauseMessageInWrapper() {
    final var cause = new IllegalArgumentException("Avro schema mismatch at field 'id'");
    final var wrapped = PipelineDiagnostics.wrap(new byte[] { 0x7b }, 0, "JsonFormat", cause);

    assertTrue(wrapped.getMessage().contains("Avro schema mismatch at field 'id'"));
    assertEquals("Avro schema mismatch at field 'id'", wrapped.getCause().getMessage());
  }
}
