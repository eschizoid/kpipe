/// Protobuf codec for KPipe.
///
/// Provides `ProtobufFormat` — a [org.kpipe.registry.MessageFormat] implementation built on
/// `com.google.protobuf` that decodes / encodes messages against a supplied descriptor — and
/// `ProtobufConsoleSink`, which renders messages as Protobuf JSON (via `protobuf-util`) for
/// debugging.
///
/// Add `kpipe-format-protobuf` to your build only when your pipeline consumes or produces
/// Protobuf messages.
package org.kpipe.format.protobuf;
