/// KPipe test module — Docker-free unit-test primitives for KPipe pipelines.
///
/// Provides [io.github.eschizoid.kpipe.test.TestStream], a `MockConsumer`-backed driver that runs
/// records through a real [io.github.eschizoid.kpipe.consumer.KPipeConsumer], and
/// [io.github.eschizoid.kpipe.test.CapturingSink] for asserting on sink deliveries. No broker, no
/// Testcontainers — tests complete in milliseconds while still exercising the production consumer
/// code paths.
module io.github.eschizoid.kpipe.test {
  requires transitive io.github.eschizoid.kpipe.core;
  requires transitive io.github.eschizoid.kpipe.consumer;
  requires transitive kafka.clients;

  exports io.github.eschizoid.kpipe.test;
}
