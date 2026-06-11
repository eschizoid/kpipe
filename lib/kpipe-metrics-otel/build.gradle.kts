plugins {
  `java-library`
  jacoco
}

description = "KPipe Metrics OTel - OpenTelemetry-backed implementation of kpipe-metrics interfaces"

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
  api(project(":lib:kpipe-metrics"))
  api(project(":lib:kpipe-core"))
  api(libs.opentelemetryApi)

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)

  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)

  // Real SDK + InMemoryMetricReader for assertions that the OTel impls actually emit metrics
  // with the right name, value, and attributes — the previous noop-only smoke tests would let
  // emission regressions land silently.
  testImplementation(libs.opentelemetrySdk)
  testImplementation(libs.opentelemetrySdkTesting)
}

tasks.test {
  useJUnitPlatform()

  if (project.hasProperty("excludeTests")) {
    val excludePattern = project.property("excludeTests").toString()
    exclude("**/${excludePattern.replace(".", "/")}.class")
  }
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
