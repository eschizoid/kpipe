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
    
    // Testcontainers for integration-style benchmarks
    implementation("org.testcontainers:testcontainers:1.20.4")
    implementation("org.testcontainers:kafka:1.20.4")
    implementation("org.testcontainers:junit-jupiter:1.20.4")
    
    jmh("org.testcontainers:testcontainers:1.20.4")
    jmh("org.testcontainers:kafka:1.20.4")
    jmh("org.testcontainers:junit-jupiter:1.20.4")
}

jmh {
    warmupIterations = 3
    iterations = 5
    fork = 1
    threads = 1
    benchmarkMode = listOf("thrpt")
    timeUnit = "s"
    failOnError = true
    forceGC = true
}

tasks.withType<JavaCompile> {
    options.release.set(25)
}
