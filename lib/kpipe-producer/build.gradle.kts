plugins {
  `java-library`
  jacoco
}

description = "KPipe Producer - Functional Kafka producer wrapper for KPipe"

dependencies {
  api(project(":lib:kpipe-core"))
  api(project(":lib:kpipe-tracing"))

  implementation(libs.kafkaClients)

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)

  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)

  testImplementation(libs.slf4jSimple)

  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainersJunitJupiter)
  testImplementation(libs.testcontainersKafka)
  testImplementation(libs.testcontainersPostgresql)
  testImplementation(libs.postgresql)
}

tasks.test {
  minHeapSize = "512m"
  maxHeapSize = "2g"
  maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(2)
}
