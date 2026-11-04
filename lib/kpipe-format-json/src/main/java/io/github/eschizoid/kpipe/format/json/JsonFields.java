package io.github.eschizoid.kpipe.format.json;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/// Operators that change the fields of a JSON record, for `.pipe(...)` on a `KPipe.json(...)`
/// stream or `.add(...)` on a JSON pipeline.
///
/// ```java
/// import static io.github.eschizoid.kpipe.format.json.JsonFields.*;
///
/// KPipe.json("orders", props)
///     .pipe(require("id"))
///     .pipe(rename("ts", "timestamp"))
///     .pipe(derive("processedAt", _ -> System.currentTimeMillis()))
///     .pipe(remove("password", "ssn"))
///     .toCustom(sink)
///     .start();
/// ```
///
/// Field names are top-level keys; `"user.id"` names a key with a dot in it, not a nested field.
/// Each operator changes the record's map in place and returns it. That is safe in a pipeline,
/// where a record's map belongs to the one attempt processing it; a retry decodes a fresh map.
///
/// An operator that cannot do what it says throws [IllegalArgumentException]. The record then
/// fails and goes to retry, the error handler and the dead-letter topic; it is never passed on
/// half-changed as a success. To skip a record instead, use
/// `Operators.filter(record -> record.containsKey("id"))`.
public final class JsonFields {

  private JsonFields() {}

  /// Sets `field` to `value`, replacing any value it had.
  ///
  /// The value is fixed when the operator is created. For a value computed per record, such as
  /// a timestamp, use [#derive].
  ///
  /// @param field the field to set
  /// @param value the value, which may be null
  /// @return an operator that sets the field
  public static UnaryOperator<Map<String, Object>> set(final String field, final Object value) {
    Objects.requireNonNull(field, "field");
    return record -> {
      record.put(field, value);
      return record;
    };
  }

  /// Sets `field` to a value computed from each record, replacing any value it had.
  ///
  /// @param field the field to set
  /// @param value computes the value from the record, before the field is set
  /// @return an operator that sets the field
  public static UnaryOperator<Map<String, Object>> derive(
    final String field,
    final Function<Map<String, Object>, Object> value
  ) {
    Objects.requireNonNull(field, "field");
    Objects.requireNonNull(value, "value");
    return record -> {
      record.put(field, value.apply(record));
      return record;
    };
  }

  /// Removes `fields`. A field the record does not have is ignored.
  ///
  /// @param fields the fields to remove
  /// @return an operator that removes the fields
  public static UnaryOperator<Map<String, Object>> remove(final String... fields) {
    final var removed = Set.copyOf(List.of(fields));
    return record -> {
      record.keySet().removeAll(removed);
      return record;
    };
  }

  /// Removes every field except `fields`. A field the record does not have is ignored.
  ///
  /// @param fields the fields to keep
  /// @return an operator that keeps only the fields
  public static UnaryOperator<Map<String, Object>> keep(final String... fields) {
    final var kept = Set.copyOf(List.of(fields));
    return record -> {
      record.keySet().retainAll(kept);
      return record;
    };
  }

  /// Moves the value of `from` to `to`.
  ///
  /// @param from the field to rename
  /// @param to the new name
  /// @return an operator that renames the field
  /// @throws IllegalArgumentException from the operator, when the record has no `from` field or
  ///     already has a `to` field, whose value the rename would otherwise overwrite
  public static UnaryOperator<Map<String, Object>> rename(final String from, final String to) {
    Objects.requireNonNull(from, "from");
    Objects.requireNonNull(to, "to");
    return record -> {
      if (!record.containsKey(from)) throw new IllegalArgumentException(
        "Cannot rename missing field '%s'".formatted(from)
      );
      if (record.containsKey(to)) throw new IllegalArgumentException(
        "Cannot rename '%s' to '%s': the record already has '%s'".formatted(from, to, to)
      );
      record.put(to, record.remove(from));
      return record;
    };
  }

  /// Passes a record on unchanged if it has every one of `fields`.
  ///
  /// @param fields the fields the record must have
  /// @return an operator that checks the fields
  /// @throws IllegalArgumentException from the operator, naming the first missing field
  public static UnaryOperator<Map<String, Object>> require(final String... fields) {
    final var required = List.of(fields);
    return record -> {
      for (final var field : required)
        if (!record.containsKey(field)) throw new IllegalArgumentException(
          "Missing required field '%s'".formatted(field)
        );
      return record;
    };
  }
}
