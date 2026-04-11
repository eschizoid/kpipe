plugins {
  java
}

java {
  modularity.inferModulePath.set(true)
}

repositories {
  mavenCentral()
}

dependencies {
  // Kafka
  implementation(libs.kafkaClients)

  // slf4j for tests if needed
  testImplementation(libs.slf4jSimple)

  // Testing
  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)

  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)

  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainersJunitJupiter)
  testImplementation(libs.testcontainersKafka)
  testImplementation(libs.testcontainersPostgresql)
  testImplementation(libs.avro)
}

tasks.test {
  useJUnitPlatform()
  minHeapSize = "1g"
  maxHeapSize = "2g"
}

tasks.compileJava {
  doFirst {
    options.compilerArgs.addAll(listOf("--module-path", classpath.asPath))
    classpath = files()
  }
}
