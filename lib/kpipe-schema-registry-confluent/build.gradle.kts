import org.pastalab.fray.gradle.FrayExtension

plugins {
  `java-library`
  jacoco
  alias(libs.plugins.fray)
}

description = "KPipe Schema Registry Confluent - Confluent Schema Registry client (schema-by-ID lookup, JSON envelope unwrap)"

dependencies {
  api(project(":lib:kpipe-core"))

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)

  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainersJunitJupiter)
  testImplementation(libs.testcontainersKafka)
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
  frayTestImplementation(project(":lib:kpipe-core"))
  "frayTestRuntimeOnly"(libs.junitPlatformLauncher)
}

// This source set compiles against the main module's exported packages as plain classpath
// classes. lib/build.gradle.kts turns modularity.inferModulePath on for every java-library
// subproject, and frayTest shares packages with main, so without this opt-out it can be pushed
// onto the module path and hit JPMS split-package errors.
tasks.named<JavaCompile>("compileFrayTestJava") {
  modularity.inferModulePath.set(false)
}

// On an unsupported OS/arch the plugin leaves this as a plain Test run rather than failing, so
// every @FrayTest reports as skipped and the build reads green. FrayInstrumentationGuardTest in
// this source set is what turns that state red.
tasks.register<Test>("frayTest") {
  group = "verification"
  description = "Runs the Fray controlled-concurrency suite for kpipe-schema-registry-confluent."
  testClassesDirs = frayTest.output.classesDirs
  classpath = frayTest.runtimeClasspath
  // Fray forks one JVM and drives scheduling itself; parallel forks would only fight over cores.
  maxParallelForks = 1
  // A fresh JVM per test class. Fray installs a global scheduler and instruments thread
  // lifecycle process-wide, so state from a finished class can leave the next one unable to
  // complete a single iteration — observed as a run that reports "Iterations: 0" forever for a
  // class that passes when run alone. Sharing one JVM across classes is the cheaper default but
  // not one this suite can rely on.
  forkEvery = 1
  testLogging { showStandardStreams = true }
}

configure<FrayExtension> {
  testTask = "frayTest"
}
