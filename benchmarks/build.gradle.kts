plugins {
  java
  alias(libs.plugins.jmh)
}

dependencies {
  // Benchmarks consume public API from :lib
  implementation(project(":lib:kpipe-consumer"))
  implementation(project(":lib:kpipe-format-json"))
  implementation(project(":lib:kpipe-format-avro"))

  // Benchmark targets
  implementation(libs.kafkaClients)
  implementation(libs.parallelConsumerCore)
  // Reactor Kafka 1.3.23 (latest stable on Maven Central as of 2026-05-16) and even the 1.4.x
  // milestones on the Spring repo still pin `kafka-clients:3.6.0`. Running against our 4.2.0
  // classpath produces a `NoSuchMethodError` on `ConsumerRecord.<init>(...)` because the ctor
  // shape changed in 4.x. The `reactor` benchmark methods are commented out for now (see
  // ParallelProcessingBenchmark and ParallelProcessingLatencyBenchmark Javadocs). The dependency
  // stays on the classpath so the `ReactorInvocationContext` keeps compiling — the day Reactor
  // Kafka ships a 4.x-compatible release we just re-enable the @Benchmark methods.
  implementation(libs.reactorKafka)
  implementation(libs.avro)
  implementation(libs.dslJson)

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
