plugins {
  `java-library`
}

java {
  modularity.inferModulePath.set(true)
}

repositories {
  mavenCentral()
}

dependencies {
  api(project(":lib:kpipe-producer"))

  // Kafka
  implementation(libs.kafkaClients)

  // DSL-JSON
  implementation(libs.dslJson)
  annotationProcessor(libs.dslJson)
  testAnnotationProcessor(libs.dslJson)

  // Avro
  implementation(libs.avro)

  // Testing
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
  useJUnitPlatform()

  minHeapSize = "4g"
  maxHeapSize = "4g"
  maxParallelForks = 1
  forkEvery = 200
}

tasks.compileJava {
  doFirst {
    options.compilerArgs.addAll(listOf("--module-path", classpath.asPath))
    classpath = files()
  }
}
