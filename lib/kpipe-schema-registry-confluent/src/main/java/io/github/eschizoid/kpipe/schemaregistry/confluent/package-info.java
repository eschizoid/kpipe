/// Client for Confluent Schema Registry's HTTP API.
///
/// - [ConfluentSchemaResolver] — [io.github.eschizoid.kpipe.registry.SchemaResolver] that fetches
/// schemas from
///   a Confluent SR over HTTP. Schemas can be looked up by integer ID (the value embedded in
///   Confluent's wire-format envelope) or by subject + version. Responses are unwrapped from
///   the SR `{"schema":"..."}` envelope so callers receive the raw schema JSON ready to hand
///   off to `AvroFormat.of(...)` or to build a `ProtobufFormat` from a generated descriptor.
/// - [CachedSchemaResolver] — in-memory caching decorator that avoids repeated HTTP round-trips
///   for hot IDs / subjects.
///
/// Add `kpipe-schema-registry-confluent` to your build only if your pipeline needs to fetch
/// schemas from a Confluent SR at runtime (or once at startup). Inline / classpath / file-based
/// schema loading is supported directly by `kpipe-format-avro` and `kpipe-format-protobuf`
/// without this module.
///
/// ```java
/// final var resolver = new ConfluentSchemaResolver("http://schema-registry:8081");
/// final var schemaJson = resolver.lookupBySubjectVersion("com.kpipe.customer", "latest");
/// final var format = AvroFormat.of(schemaJson);
/// ```
package io.github.eschizoid.kpipe.schemaregistry.confluent;
