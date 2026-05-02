/// KPipe Avro format module — provides AvroFormat, AvroMessageProcessor, and AvroConsoleSink.
///
/// Add this module to your build only if your pipeline consumes or produces Avro messages.
module org.kpipe.format.avro {
  requires transitive org.kpipe.core;
  requires org.apache.avro;
  requires com.fasterxml.jackson.core;
  requires java.net.http;
  requires dsl.json;

  exports org.kpipe.format.avro;
}
