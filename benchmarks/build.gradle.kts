plugins {
    java
    id("me.champeau.jmh") version "0.7.3"
}

val kafkaVersion = "3.9.1"
val slf4jVersion = "2.0.9"
val junitVersion = "5.10.0"

dependencies {
    // Benchmarks consume public API from :lib
    implementation(project(":lib"))

    // Benchmark targets
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    implementation("io.confluent.parallelconsumer:parallel-consumer-core:0.5.3.0")
    implementation("org.apache.avro:avro:1.12.1")
    implementation("com.dslplatform:dsl-json:2.0.2")

    // Logging for JMH forks
    implementation("org.slf4j:slf4j-simple:$slf4jVersion")

    // Apache Kafka test-kit for embedded benchmark broker
    implementation("org.apache.kafka:kafka_2.13:$kafkaVersion")
    implementation("org.apache.kafka:kafka_2.13:$kafkaVersion:test")
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion:test")
    implementation("org.apache.kafka:kafka-server-common:$kafkaVersion:test")
    implementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
}

jmh {
    warmupIterations = providers.gradleProperty("jmh.warmupIterations").orNull?.toIntOrNull() ?: 3
    iterations = providers.gradleProperty("jmh.iterations").orNull?.toIntOrNull() ?: 5
    fork = providers.gradleProperty("jmh.fork").orNull?.toIntOrNull() ?: 1
    threads = providers.gradleProperty("jmh.threads").orNull?.toIntOrNull() ?: 1

    providers
        .gradleProperty("jmh.includes")
        .orNull
        ?.split(',')
        ?.map(String::trim)
        ?.filter(String::isNotEmpty)
        ?.takeIf { it.isNotEmpty() }
        ?.let { includes = it }

    val jmhTmpDir = layout.buildDirectory.dir("tmp/jmh").get().asFile.absolutePath

    benchmarkMode = listOf("thrpt")
    timeUnit = "s"
    failOnError = true
    forceGC = true

    jvmArgs = listOf("-Djava.io.tmpdir=$jmhTmpDir")
}

tasks.withType<JavaCompile> {
    options.release.set(25)
}
