import org.pastalab.fray.gradle.FrayExtension

plugins {
  `java-library`
  jacoco
  alias(libs.plugins.fray)
}

description = "KPipe Consumer - Functional Kafka consumer with virtual threads"

dependencies {
  api(project(":lib:kpipe-core"))
  api(project(":lib:kpipe-producer"))
  api(project(":lib:kpipe-tracing"))

  implementation(libs.kafkaClients)

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)

  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)

  testImplementation(libs.jqwik)

  testImplementation(libs.slf4jSimple)

  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainersJunitJupiter)
  testImplementation(libs.testcontainersKafka)
  testImplementation(libs.testcontainersPostgresql)
  testImplementation(libs.postgresql)
}

// The Fray plugin derives its dependency configurations from the configured test-task name
// (`frayTestImplementation`, `frayTestCompileOnly`), so this source set's name is load-bearing:
// renaming it without renaming the task silently drops fray-core/fray-junit off the classpath.
val frayTest: SourceSet by sourceSets.creating {
  java.srcDir("src/frayTest/java")
  compileClasspath += sourceSets.main.get().output
  runtimeClasspath += sourceSets.main.get().output
}

val frayTestImplementation: Configuration by configurations.getting

dependencies {
  frayTestImplementation(platform(libs.junitBom))
  frayTestImplementation(libs.junitJupiter)
  frayTestImplementation(libs.kafkaClients)
  // Scenarios that build a pipeline or a batch sink need the core types by name. Declared
  // explicitly rather than by extending testImplementation, which would also drag Testcontainers
  // and the rest of the integration-test stack onto this source set.
  frayTestImplementation(project(":lib:kpipe-core"))
  "frayTestRuntimeOnly"(libs.junitPlatformLauncher)
}

// This source set compiles against the main module's exported packages as plain classpath
// classes rather than through the module path.
tasks.named<JavaCompile>("compileFrayTestJava") {
  modularity.inferModulePath.set(false)
}

// The Fray plugin rewires whichever Test task `fray.testTask` names: it points the task at the
// jlink-instrumented Fray JDK, attaches the JVMTI + bytecode agents, and narrows the JUnit
// selection to the `FrayTest` tag. On an unsupported OS/arch it skips all of that and leaves the
// task as a plain Test run — which is exactly the silent no-op the instrumentation self-check in
// this source set exists to turn red.
tasks.register<Test>("frayTest") {
  group = "verification"
  description = "Runs the Fray controlled-concurrency suite for kpipe-consumer."
  testClassesDirs = frayTest.output.classesDirs
  classpath = frayTest.runtimeClasspath
  // Fray forks one JVM and drives scheduling itself; parallel forks would only fight over cores.
  maxParallelForks = 1
  testLogging { showStandardStreams = true }
}

configure<FrayExtension> {
  testTask = "frayTest"
}

tasks.test {
  minHeapSize = "1g"
  maxHeapSize = "4g"
  maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(2)
}
