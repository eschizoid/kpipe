plugins {
  `java-library`
  jacoco
}

description = "KPipe Consumer - Functional Kafka consumer with virtual threads"

java {
  withSourcesJar()
  withJavadocJar()
  modularity.inferModulePath.set(true)
  toolchain {
    languageVersion = JavaLanguageVersion.of(25)
  }
}

repositories {
  mavenCentral()
}

dependencies {
  api(project(":lib:kpipe-core"))
  api(project(":lib:kpipe-producer"))

  implementation(libs.kafkaClients)

  // Format-specific dependencies (DSL-JSON, Avro, Protobuf) live in their dedicated modules:
  // kpipe-format-json, kpipe-format-avro, kpipe-format-protobuf.

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

  if (project.hasProperty("excludeTests")) {
    val excludePattern = project.property("excludeTests").toString()
    exclude("**/${excludePattern.replace(".", "/")}.class")
  }

  minHeapSize = "7g"
  maxHeapSize = "7g"
  maxParallelForks = 1
  forkEvery = 200
}

tasks.jacocoTestReport {
  reports {
    csv.required.set(true)
    xml.required.set(true)
    html.required.set(true)
  }
}

tasks.compileJava {
  doFirst {
    options.compilerArgs.addAll(listOf("--module-path", classpath.asPath))
    classpath = files()
  }
}

tasks.javadoc {
  options.modulePath = classpath.toList()
  classpath = files()
}
