plugins {
  java
  alias(libs.plugins.jmh)
}

// JsonPipelineBenchmark / AvroPipelineBenchmark are stale: they assume the old
// `MessagePipeline.apply(byte[])` byte-level entry point that was removed when `process()`
// returning `Result<T>` became the only pipeline output shape. They've been compile-broken on
// `main` since then, blocking every other benchmark from being built. Excluding them here so
// `ParallelProcessingBenchmark` and friends compile; rewriting the pipeline benchmarks against
// the current API is a separate follow-up.
sourceSets {
  named("jmh") {
    java {
      exclude("**/JsonPipelineBenchmark.java")
      exclude("**/AvroPipelineBenchmark.java")
    }
  }
}

dependencies {
  // Benchmarks consume public API from :lib
  implementation(project(":lib:kpipe-consumer"))
  implementation(project(":lib:kpipe-format-json"))
  implementation(project(":lib:kpipe-format-avro"))

  // Benchmark targets
  implementation(libs.kafkaClients)
  implementation(libs.parallelConsumerCore)
  // Reactor Kafka 1.3.25 (Nov 2025) was the first release that avoids the deprecated
  // ConsumerRecord ctor that was removed in kafka-clients 4.x. Its POM still pins
  // kafka-clients:3.9.1 but the new binary works when Gradle conflict-resolves the classpath
  // to our 4.2.0. No `exclude` needed; conflict resolution picks the higher version.
  implementation(libs.reactorKafka)
  implementation(libs.avro)

  // Logging for JMH forks
  implementation(libs.slf4jSimple)

  // Broker for the bench. Testcontainers boots a real Kafka 4.2.0 container in Docker, so the
  // broker runs in its own JVM on its own cores instead of competing with the consumer under
  // test for CPU. The old in-process `kafka-test-common-runtime` harness collapsed under load
  // (consumer + KRaft controller + group coordinator on the same cores).
  implementation(libs.testcontainersKafka)

  implementation(libs.junitJupiterApi)
}

jmh {
  fun intProp(
    name: String,
    default: Int,
  ): Int = providers.gradleProperty(name).orNull?.toIntOrNull() ?: default

  fun stringProp(
    name: String,
    default: String,
  ): String = providers
    .gradleProperty(name)
    .orNull
    ?.trim()
    ?.takeIf { it.isNotEmpty() } ?: default

  fun csvProp(name: String): List<String>? = providers
    .gradleProperty(name)
    .orNull
    ?.split(',')
    ?.map(String::trim)
    ?.filter(String::isNotEmpty)
    ?.takeIf { it.isNotEmpty() }

  warmupIterations = intProp("jmh.warmupIterations", 3)
  iterations = intProp("jmh.iterations", 5)
  fork = intProp("jmh.fork", 1)
  threads = intProp("jmh.threads", 1)

  csvProp("jmh.includes")?.let { includes = it }
  csvProp("jmh.profilers")?.let { profilers = it }

  val jmhResultFormat = stringProp("jmh.resultFormat", "TEXT")
  val jmhTmpDir =
    layout.buildDirectory
      .dir("tmp/jmh")
      .get()
      .asFile.absolutePath
  val jmhResultFile =
    layout.buildDirectory
      .file("results/jmh/results.${jmhResultFormat.lowercase()}")
      .get()
      .asFile

  benchmarkMode = listOf("thrpt")
  timeUnit = "s"
  failOnError = true
  forceGC = true
  resultFormat = jmhResultFormat
  resultsFile = jmhResultFile

  jvmArgs = listOf("-Djava.io.tmpdir=$jmhTmpDir")
}

tasks.withType<JavaCompile> {
  options.release.set(25)
}
