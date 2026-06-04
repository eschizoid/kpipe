/// Avro codec for KPipe.
///
/// Provides `AvroFormat` — a [org.kpipe.registry.MessageFormat] implementation built on
/// `org.apache.avro` that decodes / encodes records against a supplied schema — and
/// `AvroConsoleSink`, which renders records as Avro JSON for debugging. Schemas can be
/// supplied inline, loaded from classpath / file, or resolved at runtime via
/// `kpipe-schema-registry-confluent`.
///
/// Add `kpipe-format-avro` to your build only when your pipeline consumes or produces Avro
/// messages.
package org.kpipe.format.avro;
