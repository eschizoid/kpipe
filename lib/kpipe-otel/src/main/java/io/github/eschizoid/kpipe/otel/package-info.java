/// KPipe on OpenTelemetry. [KPipeOtel] is the entry point: [KPipeOtel#tracer] traces each
/// consumed record and carries its trace into every record written on its behalf,
/// [KPipeOtel#metrics] exports consumers' metrics, and [KPipeOtel#schemaRegistryCache] exports a
/// Schema Registry cache.
///
/// The only OpenTelemetry artifact this module depends on is `opentelemetry-api`. Bring an SDK,
/// such as `io.opentelemetry:opentelemetry-sdk`, and an exporter.
package io.github.eschizoid.kpipe.otel;
