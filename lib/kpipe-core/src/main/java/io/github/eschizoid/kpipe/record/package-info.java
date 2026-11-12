/// The record being processed.
///
/// [RecordContext] gives operators and sinks the key, headers, topic, partition, offset and
/// timestamp of the record they are processing, through [RecordContext#current()]. It holds no
/// Kafka types; the consumer module adapts a consumed record to it.
package io.github.eschizoid.kpipe.record;
