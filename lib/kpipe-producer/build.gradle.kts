plugins {
  `java-library`
  jacoco
}

description = "KPipe Producer - Functional Kafka producer wrapper for KPipe"

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
  api(project(":lib:kpipe-metrics"))

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
  useJUnitPlatform()

  if (project.hasProperty("excludeTests")) {
    val excludePattern = project.property("excludeTests").toString()
    exclude("**/${excludePattern.replace(".", "/")}.class")
  }

  minHeapSize = "512m"
  maxHeapSize = "2g"
  maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(2)
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
