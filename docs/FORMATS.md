# Formats and Schema Registry

Payload formats, their typed pipelines, wire envelopes, and Confluent Schema Registry integration. Snippets use
placeholder values unless stated otherwise.

Pipelines deserialize once, transform many times, serialize once. Operators are `UnaryOperator<T>` where `T` is the
format's payload type: `Map<String, Object>` for JSON, `GenericRecord` for Avro, `Message` for Protobuf.

A note on mutability: the **pipeline definition** is immutable (each fluent call returns a new `Stream<T>`), but the
**payload objects** are not necessarily — JSON maps and Avro records are mutable, and several `Operators` helpers
mutate them in place. That is safe because one record's payload is confined to the single worker processing it, and a
retry re-deserializes from the raw bytes rather than reusing a possibly-mutated object. Protobuf messages are
immutable; transforms build a new message via `toBuilder()`.

## JSON

Add `kpipe-format-json` (parsing backed by [fastjson2](https://github.com/alibaba/fastjson2)). Operators are
`UnaryOperator<Map<String, Object>>`:

```java
import static io.github.eschizoid.kpipe.registry.Operators.*;

final var registry = new MessageProcessorRegistry();

final var sanitizeKey = RegistryKey.json("sanitize");
registry.registerOperator(sanitizeKey, removeFields("password", "ssn"));

// Inline lambdas work too. Returning null from an operator filters the record: downstream
// sinks are skipped but the offset is still marked processed.
final var lowerEmailKey = RegistryKey.json("lowerEmail");
registry.registerOperator(lowerEmailKey, msg -> {
  if (msg.get("email") instanceof String s) msg.put("email", s.toLowerCase());
  return msg;
});

final var pipeline = registry.pipeline(JsonFormat.INSTANCE)
    .add(sanitizeKey)
    .add(lowerEmailKey)
    .build();
```

## Avro

Add `kpipe-format-avro`. Operators are `UnaryOperator<GenericRecord>`. Two construction modes, fixed at build time:

- **Static schema** — `new AvroFormat(schema)` for a parsed `Schema`, or `AvroFormat.of(schemaJson)` for inline JSON.
  Use when the schema is on your classpath and evolution is strictly append-only.
- **Registry mode** — `AvroFormat.withRegistry(resolver)`, or the fluent `KPipe.avro(topic, props, resolver)` /
  `.withSchemaRegistry(resolver)`. Per-record writer-schema lookup; see below.

```java
final var format = AvroFormat.of("""
  {"type":"record","name":"User","namespace":"com.kpipe","fields":[
    {"name":"id","type":"string"},{"name":"name","type":"string"}
  ]}""");

final var registry = new MessageProcessorRegistry();
final var lowerNameKey = RegistryKey.of("lowerName", GenericRecord.class);
registry.registerOperator(lowerNameKey, record -> {
  if (record.get("name") != null) record.put("name", record.get("name").toString().toLowerCase());
  return record;
});
```

## Protobuf

Add `kpipe-format-protobuf`. Operators are `UnaryOperator<Message>`; messages are immutable, so transforms build a new
message:

```java
final var format = new ProtobufFormat(CustomerProto.Customer.getDescriptor());
final var registry = new MessageProcessorRegistry();

final var clearEmailKey = RegistryKey.of("clearEmail", Message.class);
registry.registerOperator(clearEmailKey, msg -> {
  final var emailField = msg.getDescriptorForType().findFieldByName("email");
  return msg.toBuilder().clearField(emailField).build();
});
```

## Wire envelopes and skipBytes

Records produced through Confluent serializers carry a wire envelope in front of the payload:

- **Avro**: 1 magic byte + 4-byte schema ID = a fixed **5-byte** prefix.
- **Protobuf**: 1 magic byte + 4-byte schema ID + a **variable-length message-index list** (zig-zag varints
  identifying which message in the `.proto` file was written). The common single-top-level-message case is encoded as
  the one-byte shorthand `0x00`, making the envelope 6 bytes — but nested or non-first message types produce a longer
  index, and the length is not knowable up front.

`Stream.skipBytes(n)` strips a **fixed-length** prefix before deserialization (copying the payload once, minus the
prefix). That makes it a low-level escape hatch with sharp edges:

- `skipBytes(5)` + a static `AvroFormat` decodes Confluent-framed Avro **only** while writer and reader schema are
  identical — it does not resolve the writer schema, so producer-side evolution can silently mis-decode. Prefer
  registry mode, which reads the envelope itself and resolves the actual writer schema per record.
- `skipBytes(6)` + a static `ProtobufFormat` is correct **only** for the single-byte `0x00` message-index shorthand
  (first top-level message). It is **not** a general Confluent Protobuf decoder — any other message-index corrupts the
  decode. For Confluent-framed Protobuf, use registry mode, which parses the variable-length index properly.
- Combining `skipBytes` with `withSchemaRegistry` is rejected at build time (the registry format already consumes the
  envelope; stripping it first would destroy the schema ID).

Legitimate `skipBytes` uses: proprietary fixed-size headers, and the identical-schema Avro case above when you accept
its constraint.

## Confluent Schema Registry mode

For deployments where schemas live in the registry, add `kpipe-schema-registry-confluent` and use per-record
auto-lookup. The format reads each record's envelope, resolves the writer's schema by ID (cached in-process — IDs are
immutable in Confluent SR, so the cache needs no TTL), and decodes against the actual writer schema. This is the
schema-evolution-correct path: producer evolution cannot silently corrupt consumer output the way a
static-fetch-at-startup pattern can.

```java
try (final var resolver = new CachedSchemaResolver(
        new ConfluentSchemaResolver("http://schema-registry:8081", Duration.ofSeconds(10)));
     final var handle = KPipe.avro("orders", kafkaProps, resolver)
         .pipe(record -> enrich(record))
         .toCustom(orderSink)
         .start()) {
  handle.awaitShutdown();
}
```

The same shape works for Protobuf via `KPipe.protobuf(topic, props, resolver)` — it additionally requires
`kpipe-format-protobuf-confluent` on the runtime classpath (Confluent SR stores Protobuf schemas as `.proto` source
text; that module carries the shaded compiler for it, and its output is wire-compatibility-tested against Confluent's
own `KafkaProtobufSerializer`).

Registry-mode details that matter in practice:

- Only the first record carrying a new schema ID costs an HTTP round trip; the resolver exposes hit/miss/size counters
  you can bind to metrics ([OBSERVABILITY.md](OBSERVABILITY.md#pipeline-outcome-counters)).
- Registry mode is consumer-side only: `serialize` is unsupported, and `toConsole()` throws (there is no fixed schema
  to format with) — use `toCustom(...)`.
- No schema publishing and no compatibility checking happen in KPipe; both belong to the producer client and the
  registry itself.
- A `SchemaResolver` failure (registry down) propagates as a record-processing failure — retry/DLQ semantics apply,
  per [GUARANTEES.md](GUARANTEES.md#failure-matrix).
