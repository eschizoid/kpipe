package io.github.eschizoid.kpipe.registry;

/// SPI for fetching schema JSON by the integer ID embedded in a wire-format envelope.
///
/// Implementations typically talk to a remote schema registry (e.g. Confluent SR via the
/// `kpipe-schema-registry-confluent` module). The contract is intentionally narrow — just
/// "id → schema JSON" — so format modules (`kpipe-format-avro`, `kpipe-format-protobuf`) can stay
/// agnostic of any particular registry implementation and the SPI carries no third-party types.
///
/// Implementations should be thread-safe (resolvers are typically shared across virtual threads
/// in a hot path) and idempotent for repeated lookups of the same id (callers may cache
/// internally, but a resolver-level cache is the common shape).
public interface SchemaResolver {
  /// Returns the raw schema JSON for `schemaId`, or throws if the schema is not found or the
  /// lookup fails. Implementations must NOT return null — distinguish "not found" from "fetched
  /// successfully but empty" by throwing a clear exception.
  ///
  /// @param schemaId the integer schema identifier (typically extracted from the wire envelope)
  /// @return the schema JSON
  /// @throws RuntimeException if the lookup fails or the schema is not found
  String lookupById(int schemaId);
}
