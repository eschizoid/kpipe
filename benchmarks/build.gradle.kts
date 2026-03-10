import org.gradle.api.artifacts.VersionCatalogsExtension

plugins {
    java
    id("me.champeau.jmh") version "0.7.3"
}

val libsCatalog = rootProject.extensions.getByType<VersionCatalogsExtension>().named("libs")

dependencies {
    // Benchmarks consume public API from :lib
    implementation(project(":lib"))

    // Benchmark targets
    implementation(libsCatalog.findLibrary("kafkaClients").get())
    implementation(libsCatalog.findLibrary("parallelConsumerCore").get())
    implementation(libsCatalog.findLibrary("avro").get())
    implementation(libsCatalog.findLibrary("dslJson").get())

    // Logging for JMH forks
    implementation(libsCatalog.findLibrary("slf4jSimple").get())

    // Apache Kafka test-kit for embedded benchmark broker
    val kafkaVersion = libsCatalog.findVersion("kafka").get().requiredVersion
    implementation(libsCatalog.findLibrary("kafkaScala213").get())
    implementation("org.apache.kafka:kafka_2.13:$kafkaVersion:test")
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion:test")
    implementation("org.apache.kafka:kafka-server-common:$kafkaVersion:test")
    implementation(libsCatalog.findLibrary("junitJupiterApi").get())
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
