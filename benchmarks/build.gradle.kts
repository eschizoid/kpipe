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
  // Reactor Kafka 1.3.25 (Nov 2025) was the first release that avoids the deprecated
  // ConsumerRecord ctor that was removed in kafka-clients 4.x. Its POM still pins
  // kafka-clients:3.9.1 but the new binary works when Gradle conflict-resolves the classpath
  // to our 4.3.0. No `exclude` needed; conflict resolution picks the higher version.
  implementation(libs.reactorKafka)
  implementation(libs.avro)

  // Logging for JMH forks
  implementation(libs.slf4jSimple)

  // Broker for the bench. Testcontainers boots a real Kafka 4.3.0 container in Docker, so the
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

  // Override the hardcoded `workMicros` @Param at runtime. The full sweep
  // (0/100/1000/10000/35000/50000/100000) is an overnight run because the single-threaded arm is
  // serial — 25k records × 100ms = ~42 min per iteration — which overflows CI's job cap. Pass
  // `-Pjmh.workMicros=0,100,1000` to run only the publishable cells (the TEMPLATE reports exactly
  // those three) so a capture fits in CI. Empty = the benchmark's own @Param default (full sweep).
  csvProp("jmh.workMicros")?.let { values ->
    benchmarkParameters.put("workMicros", objects.listProperty<String>().value(values))
  }

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
