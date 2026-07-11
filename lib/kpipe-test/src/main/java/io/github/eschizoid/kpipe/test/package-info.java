/// Docker-free unit-test primitives for KPipe pipelines.
///
/// The entry point is [io.github.eschizoid.kpipe.test.TestStream]: build a pipeline with the same
/// `pipe` / `filter` / `toCustom` vocabulary as the production facade, `send` typed records, call
/// `flush()` for a deterministic drain, and assert on a
/// [io.github.eschizoid.kpipe.test.CapturingSink].
///
/// ```java
/// final var captured = new CapturingSink<Map<String, Object>>();
/// try (final var driver = TestStream.<Map<String, Object>>builder(JsonFormat.INSTANCE)
///     .pipe(addTimestamp)
///     .filter(active)
///     .toCustom(captured)
///     .build()) {
///   driver.send(record1);
///   driver.send(record2);
///   driver.flush();
///   assertEquals(List.of(expected), captured.captured());
/// }
/// ```
package io.github.eschizoid.kpipe.test;
