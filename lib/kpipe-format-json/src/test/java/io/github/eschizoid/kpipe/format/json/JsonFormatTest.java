package io.github.eschizoid.kpipe.format.json;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class JsonFormatTest {

  @Test
  void deserializeReturnsNullForNullOrEmptyBytes() {
    assertNull(JsonFormat.INSTANCE.deserialize(null));
    assertNull(JsonFormat.INSTANCE.deserialize(new byte[0]));
  }

  @Test
  void deserializeErrorMessageCarriesFormatNameAndByteLength() {
    final var malformed = "{\"a\":".getBytes(StandardCharsets.UTF_8);
    final var thrown = assertThrows(RuntimeException.class, () -> JsonFormat.INSTANCE.deserialize(malformed));
    final var message = thrown.getMessage();
    assertTrue(message.contains("JsonFormat"), () -> "message should name the format: " + message);
    assertTrue(
      message.contains(malformed.length + " bytes"),
      () -> "message should report the byte length: " + message
    );
  }

  @Test
  void deserializeErrorMessageHandlesShortPayloadWithoutThrowing() {
    final var shortMalformed = new byte[] { (byte) 0xff, (byte) 0xfe };
    final var thrown = assertThrows(RuntimeException.class, () -> JsonFormat.INSTANCE.deserialize(shortMalformed));
    assertTrue(thrown.getMessage().contains("2 bytes"), () -> "message: " + thrown.getMessage());
  }
}
