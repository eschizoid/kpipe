plugins {
  `java-library`
  jacoco
}

java {
  modularity.inferModulePath.set(true)
  toolchain {
    languageVersion = JavaLanguageVersion.of(25)
  }
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

afterEvaluate {
  tasks.withType<Jar>().configureEach {
    archiveBaseName.set("kpipe")
  }
}

tasks.compileJava {
  doFirst {
    options.compilerArgs.addAll(listOf("--module-path", classpath.asPath))
    classpath = files()
  }
}
