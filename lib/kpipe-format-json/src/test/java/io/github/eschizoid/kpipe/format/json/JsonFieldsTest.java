package io.github.eschizoid.kpipe.format.json;

import static io.github.eschizoid.kpipe.format.json.JsonFields.*;
import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.Result;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class JsonFieldsTest {

  private static Map<String, Object> record(final Object... keysAndValues) {
    final var record = new HashMap<String, Object>();
    for (int i = 0; i < keysAndValues.length; i += 2) record.put((String) keysAndValues[i], keysAndValues[i + 1]);
    return record;
  }

  @Test
  void setAddsOrReplacesAField() {
    assertEquals(Map.of("a", 1, "b", 2), set("b", 2).apply(record("a", 1)));
    assertEquals(Map.of("a", 9), set("a", 9).apply(record("a", 1)));
  }

  @Test
  void deriveComputesTheValueForEachRecord() {
    final var counter = new AtomicLong();
    final var stamp = derive("seq", _ -> counter.incrementAndGet());

    assertEquals(1L, stamp.apply(record()).get("seq"));
    assertEquals(2L, stamp.apply(record()).get("seq"), "a fixed value would repeat");
  }

  @Test
  void deriveReadsTheRecordBeforeSettingTheField() {
    final var total = derive("total", r -> (Integer) r.get("price") * (Integer) r.get("qty"));

    assertEquals(Map.of("price", 3, "qty", 4, "total", 12), total.apply(record("price", 3, "qty", 4)));
  }

  @Test
  void removeDropsTheFieldsAndIgnoresAbsentOnes() {
    assertEquals(Map.of("id", 1), remove("password", "ssn").apply(record("id", 1, "password", "x")));
  }

  @Test
  void keepDropsEveryOtherField() {
    assertEquals(Map.of("id", 1), keep("id", "name").apply(record("id", 1, "password", "x")));
  }

  @Test
  void aRepeatedFieldNameIsAccepted() {
    assertEquals(Map.of(), remove("a", "a").apply(record("a", 1)));
    assertEquals(Map.of("a", 1), keep("a", "a").apply(record("a", 1, "b", 2)));
  }

  @Test
  void renameMovesTheValue() {
    assertEquals(Map.of("timestamp", 5), rename("ts", "timestamp").apply(record("ts", 5)));
  }

  @Test
  void renameOfAMissingFieldFails() {
    final var thrown = assertThrows(IllegalArgumentException.class, () ->
      rename("ts", "timestamp").apply(record("id", 1))
    );
    assertTrue(thrown.getMessage().contains("ts"), thrown.getMessage());
  }

  @Test
  void renameOntoAnExistingFieldFailsInsteadOfOverwritingIt() {
    final var input = record("ts", 5, "timestamp", 7);

    assertThrows(IllegalArgumentException.class, () -> rename("ts", "timestamp").apply(input));
    assertEquals(Map.of("ts", 5, "timestamp", 7), input, "the record is left as it was");
  }

  @Test
  void requirePassesARecordThatHasEveryField() {
    final var input = record("id", 1, "name", "a");

    assertSame(input, require("id", "name").apply(input));
  }

  @Test
  void requireFailsNamingTheFirstMissingField() {
    final var thrown = assertThrows(IllegalArgumentException.class, () ->
      require("id", "name", "email").apply(record("id", 1))
    );
    assertTrue(thrown.getMessage().contains("'name'"), thrown.getMessage());
  }

  @Test
  void aFailingFieldOperatorFailsTheRecordInAPipeline() {
    final var pipeline = new MessageProcessorRegistry().pipeline(JsonFormat.INSTANCE).add(require("id")).build();
    final var decoded = pipeline.deserializeOrFail("{\"name\":\"a\"}".getBytes(StandardCharsets.UTF_8));

    final var failed = assertInstanceOf(Result.Failed.class, pipeline.process(decoded));
    assertInstanceOf(IllegalArgumentException.class, failed.cause());
  }
}
