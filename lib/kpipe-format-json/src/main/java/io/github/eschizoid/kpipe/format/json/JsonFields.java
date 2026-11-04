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
///     .pipe(setIfAbsent("currency", "USD"))
///     .pipe(derive("processedAt", _ -> System.currentTimeMillis()))
///     .pipe(remove("password", "ssn"))
///     .toCustom(sink)
///     .start();
/// ```
///
/// Field names are top-level keys; `"user.id"` names a key with a dot in it, not a nested field.
/// A field whose value is JSON `null` counts as missing: [JsonFormat] leaves null-valued keys out
/// when it writes a record. Each operator changes the record's map in place and returns it. That
/// is safe in a pipeline, where a record's map belongs to the one attempt processing it; a retry
/// decodes a fresh map.
///
/// An operator that cannot do what it says on a record throws [IllegalArgumentException]. The
/// record then fails and goes to retry, the error handler and the dead-letter topic when one is
/// configured; it is never passed on half-changed as a success. To skip a record instead, use
/// `Operators.filter(record -> record.get("id") != null)`. Arguments that could never work on any
/// record, such as an empty field list, are rejected when the operator is created.
public final class JsonFields {

  private JsonFields() {}

  /// Sets `field` to `value`, replacing any value it had.
  ///
  /// The same `value` object goes into every record, so a mutable value is shared between them.
  /// For a value computed or created per record, such as a timestamp or a fresh list, use
  /// [#derive].
  ///
  /// @param field the field to set
  /// @param value the value
  /// @return an operator that sets the field
  /// @throws NullPointerException if `value` is null; use [#remove] to drop a field
  public static UnaryOperator<Map<String, Object>> set(final String field, final Object value) {
    Objects.requireNonNull(field, "field");
    Objects.requireNonNull(value, "value; to drop a field, use remove");
    return record -> {
      record.put(field, value);
      return record;
    };
  }

  /// Sets `field` to `value` if the record has no value for it, and leaves it alone otherwise.
  /// Use it for a default when older records lack a newer field.
  ///
  /// @param field the field to default
  /// @param value the default value, shared between records as in [#set]
  /// @return an operator that sets the field when it is missing
  /// @throws NullPointerException if `value` is null
  public static UnaryOperator<Map<String, Object>> setIfAbsent(final String field, final Object value) {
    Objects.requireNonNull(field, "field");
    Objects.requireNonNull(value, "value");
    return record -> {
      if (record.get(field) == null) record.put(field, value);
      return record;
    };
  }

  /// Sets `field` to a value computed from each record, replacing any value it had. A null result
  /// leaves the field out when the record is written.
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
  /// @param fields the fields to remove, at least one
  /// @return an operator that removes the fields
  /// @throws IllegalArgumentException if no field is given
  public static UnaryOperator<Map<String, Object>> remove(final String... fields) {
    final var removed = Set.copyOf(fieldList("remove", fields));
    return record -> {
      record.keySet().removeAll(removed);
      return record;
    };
  }

  /// Removes every field except `fields`. A field the record does not have is ignored.
  ///
  /// @param fields the fields to keep, at least one
  /// @return an operator that keeps only the fields
  /// @throws IllegalArgumentException if no field is given, which would empty every record
  public static UnaryOperator<Map<String, Object>> keep(final String... fields) {
    final var kept = Set.copyOf(fieldList("keep", fields));
    return record -> {
      record.keySet().retainAll(kept);
      return record;
    };
  }

  /// Moves the value of `from` to `to`. To replace an existing `to`, remove it first:
  /// `.pipe(remove("timestamp")).pipe(rename("ts", "timestamp"))`.
  ///
  /// @param from the field to rename
  /// @param to the new name, different from `from`
  /// @return an operator that renames the field
  /// @throws IllegalArgumentException if `from` and `to` are the same; and from the operator, when
  ///     the record has no `from` field or already has a `to` field, whose value the rename
  ///     would otherwise overwrite
  public static UnaryOperator<Map<String, Object>> rename(final String from, final String to) {
    Objects.requireNonNull(from, "from");
    Objects.requireNonNull(to, "to");
    if (from.equals(to)) throw new IllegalArgumentException("Cannot rename '%s' to itself".formatted(from));
    return record -> {
      if (record.get(from) == null) throw new IllegalArgumentException(
        "Cannot rename missing field '%s'".formatted(from)
      );
      if (record.get(to) != null) throw new IllegalArgumentException(
        "Cannot rename '%s' to '%s': the record already has '%s'".formatted(from, to, to)
      );
      record.remove(to);
      record.put(to, record.remove(from));
      return record;
    };
  }

  /// Passes a record on unchanged if it has a non-null value for every one of `fields`.
  ///
  /// @param fields the fields the record must have, at least one
  /// @return an operator that checks the fields
  /// @throws IllegalArgumentException if no field is given; and from the operator, naming the
  ///     first field in `fields` that is missing or null
  public static UnaryOperator<Map<String, Object>> require(final String... fields) {
    final var required = fieldList("require", fields);
    return record -> {
      for (final var field : required)
        if (record.get(field) == null) throw new IllegalArgumentException(
          "Missing required field '%s'".formatted(field)
        );
      return record;
    };
  }

  private static List<String> fieldList(final String operator, final String... fields) {
    Objects.requireNonNull(fields, "fields");
    if (fields.length == 0) throw new IllegalArgumentException("%s needs at least one field".formatted(operator));
    for (final var field : fields) Objects.requireNonNull(field, "field names cannot be null");
    return List.of(fields);
  }
}
