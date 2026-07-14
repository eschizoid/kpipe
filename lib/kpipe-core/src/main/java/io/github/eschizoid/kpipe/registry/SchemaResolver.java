package io.github.eschizoid.kpipe.registry;

/// SPI for fetching a schema's text by the integer ID embedded in a wire-format envelope.
///
/// Implementations typically talk to a remote schema registry (e.g. Confluent SR via the
/// `kpipe-schema-registry-confluent` module). The contract is intentionally narrow — just
/// "id → schema text" — so format modules (`kpipe-format-avro`, `kpipe-format-protobuf`) can stay
/// agnostic of any particular registry implementation and the SPI carries no third-party types.
/// The text is the format's native schema representation: Avro JSON, or Protobuf `.proto` source.
///
/// Implementations should be thread-safe (resolvers are typically shared across virtual threads
/// in a hot path) and idempotent for repeated lookups of the same id (callers may cache
/// internally, but a resolver-level cache is the common shape).
public interface SchemaResolver {
  /// Returns the raw schema text for `schemaId` (Avro JSON or Protobuf `.proto` source), or throws
  /// if the schema is not found or the lookup fails. Implementations must NOT return null —
  /// distinguish "not found" from "fetched successfully but empty" by throwing a clear exception.
  ///
  /// @param schemaId the integer schema identifier (typically extracted from the wire envelope)
  /// @return the schema text (Avro JSON or Protobuf `.proto` source)
  /// @throws RuntimeException if the lookup fails or the schema is not found
  String lookupById(int schemaId);
}
