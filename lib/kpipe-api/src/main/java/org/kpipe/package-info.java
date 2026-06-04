/// KPipe public API — user-facing entry point.
///
/// Provides the fluent [KPipe] factory plus the [Stream], [Sink], and [Handle] types that
/// together let you write a complete consumer pipeline in a handful of lines. A single
/// dependency on `kpipe-api` transitively pulls in `kpipe-core`, `kpipe-consumer`,
/// `kpipe-producer`, and the JSON / Avro / Protobuf format modules.
///
/// Typical usage:
/// ```java
/// KPipe.stream("topic", JsonFormat.of())
///      .process(...)
///      .to(Sink.console())
///      .start();
/// ```
package org.kpipe;
