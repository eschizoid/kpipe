plugins {
    java
    id("me.champeau.jmh") version "0.7.3"
}

dependencies {
    implementation(project(":lib"))
    implementation("org.apache.kafka:kafka-clients:3.9.1")
    implementation("io.confluent.parallelconsumer:parallel-consumer-core:0.5.3.0")

    // SLF4J implementation for JMH runs
    implementation("org.slf4j:slf4j-simple:2.0.9")

    implementation("org.apache.avro:avro:1.12.1")
    implementation("com.dslplatform:dsl-json:2.0.2")
    implementation("org.openjdk.jmh:jmh-core:1.37")
    annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.37")

    // JMH dependencies
    jmh("org.openjdk.jmh:jmh-core:1.37")
    jmh("org.openjdk.jmh:jmh-generator-annprocess:1.37")
    jmh("org.slf4j:slf4j-simple:2.0.9")

    // Apache Kafka test-kit for embedded benchmark broker
    implementation("org.apache.kafka:kafka_2.13:3.9.1")
    implementation("org.apache.kafka:kafka_2.13:3.9.1:test")
    implementation("org.apache.kafka:kafka-clients:3.9.1:test")
    implementation("org.apache.kafka:kafka-server-common:3.9.1:test")
    implementation("org.junit.jupiter:junit-jupiter-api:5.10.0")
    jmh("org.apache.kafka:kafka_2.13:3.9.1")
    jmh("org.apache.kafka:kafka_2.13:3.9.1:test")
    jmh("org.apache.kafka:kafka-clients:3.9.1:test")
    jmh("org.apache.kafka:kafka-server-common:3.9.1:test")
    jmh("org.junit.jupiter:junit-jupiter-api:5.10.0")
}

fun intGradleProp(name: String, defaultValue: Int): Int {
    return providers.gradleProperty(name).orNull?.toIntOrNull() ?: defaultValue
}

fun listGradleProp(name: String): List<String> {
    return providers
        .gradleProperty(name)
        .orNull
        ?.split(',')
        ?.map { it.trim() }
        ?.filter { it.isNotEmpty() }
        ?: emptyList()
}

jmh {
    warmupIterations = intGradleProp("jmh.warmupIterations", 3)
    iterations = intGradleProp("jmh.iterations", 5)
    fork = intGradleProp("jmh.fork", 1)
    threads = intGradleProp("jmh.threads", 1)

    val includePatterns = listGradleProp("jmh.includes")
    if (includePatterns.isNotEmpty()) {
        includes = includePatterns
    }

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
