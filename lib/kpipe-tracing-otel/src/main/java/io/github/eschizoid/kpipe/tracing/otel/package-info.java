/// OpenTelemetry implementation of the [io.github.eschizoid.kpipe.tracing.Tracer] SPI.
///
/// [OtelTracer] starts a `CONSUMER` span named `process <topic>` for each consumed record, as a
/// child of the trace context in the record's headers, with the OpenTelemetry messaging
/// attributes. Each failed attempt adds a `kpipe.attempt.failed` event, and a final failure
/// records the exception and marks the span as an error. Spans report under the instrumentation
/// scope [OtelTracer#INSTRUMENTATION_SCOPE].
///
/// Context is read from and written to Kafka headers through the text-map propagator of the
/// `OpenTelemetry` instance, so configure one, such as W3C trace context. With none, nothing
/// propagates and the constructor logs a warning.
///
/// ```java
/// KPipe.json("orders", props)
///   .withTracer(new OtelTracer(openTelemetry))
///   .toKafka("orders-enriched");
/// ```
///
/// The only OpenTelemetry artifact this module depends on is `opentelemetry-api`. Bring an SDK,
/// such as `io.opentelemetry:opentelemetry-sdk`, and an exporter.
package io.github.eschizoid.kpipe.tracing.otel;
