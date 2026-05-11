/// KPipe Avro format module — provides AvroFormat and AvroConsoleSink.
///
/// Add this module to your build only if your pipeline consumes or produces Avro messages.
module org.kpipe.format.avro {
  requires transitive org.kpipe.core;
  requires org.apache.avro;
  // Jackson stays — Avro's EncoderFactory.jsonEncoder transitively exposes Jackson types in
  // AvroConsoleSink. The HTTP fetcher's direct use of JsonFactory / JsonToken has moved to
  // kpipe-schema-registry-confluent, but Avro itself still pulls Jackson at compile time.
  requires com.fasterxml.jackson.core;

  exports org.kpipe.format.avro;
}
