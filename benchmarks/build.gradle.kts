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
  // Reactor Kafka pulls a transitive kafka-clients; force the project version via the
  // `kafkaClients` dependency above so all consumers under test hit the same wire stack.
  implementation(libs.reactorKafka) {
    exclude(group = "org.apache.kafka", module = "kafka-clients")
  }
  implementation(libs.avro)
  implementation(libs.dslJson)

  // Logging for JMH forks
  implementation(libs.slf4jSimple)

  implementation(libs.kafkaTestCommonRuntime)

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
