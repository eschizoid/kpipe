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

// jcstress concurrency-stress harness. Lives in its own source set so the generated test
// runner and annotation-processed scaffolding stay isolated from the JUnit test source set,
// and so it runs purely on the classpath (jcstress instruments bytecode at runtime and does
// not support the Java module path).
val jcstress: SourceSet by sourceSets.creating {
  java.srcDir("src/jcstress/java")
  compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
  runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
}

val jcstressImplementation: Configuration by configurations.getting {
  extendsFrom(configurations.testImplementation.get())
}
configurations["jcstressRuntimeOnly"].extendsFrom(configurations.testRuntimeOnly.get())
configurations["jcstressAnnotationProcessor"].extendsFrom(configurations["annotationProcessor"])

dependencies {
  jcstressImplementation(libs.jcstressCore)
  "jcstressAnnotationProcessor"(libs.jcstressCore)
}

// jcstress compiles against the classpath, not the module path. The main module's exported
// packages are reachable as plain classpath classes, so no --module-path wiring is needed.
tasks.named<JavaCompile>("compileJcstressJava") {
  modularity.inferModulePath.set(false)
}

// Runnable harness. Iterations are capped hard via -t/-iters/-time so a single invocation
// proves jcstress executes on JDK 25 without launching a multi-minute campaign.
tasks.register<JavaExec>("jcstress") {
  group = "verification"
  description = "Runs the jcstress concurrency harness for kpipe-consumer."
  classpath = jcstress.runtimeClasspath
  mainClass.set("org.openjdk.jcstress.Main")
  jvmArgs("-Djdk.attach.allowAttachSelf=true")
  // Keep jcstress scratch files (results .bin.gz, HTML report) under build/ instead of the module dir.
  val outDir = layout.buildDirectory.dir("jcstress").get().asFile
  doFirst { outDir.mkdirs() }
  workingDir = outDir
  // -t matches every jcstress test in the consumer package by its shared JCStressTest suffix.
  args("-t", "JCStressTest", "-iters", "1", "-time", "50", "-f", "1", "-v", "-r", "results")
}

// Fray controlled-concurrency pilot. Mirrors the jcstress layout above: its own source set so
// the Fray tests never run inside the plain `test` task, and classpath-only compilation so the
// tests can reach package-private consumer internals (KeyOrderedDispatcher.tombstoneRetries)
// without reflection and without a module-path dance.
//
// The source set MUST be named `frayTest`: the Fray plugin derives the configuration names it
// injects its own dependencies into from the configured test-task name (`frayTestImplementation`,
// `frayTestCompileOnly`). Renaming one without the other silently drops fray-core/fray-junit off
// the compile classpath.
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

// Same rationale as compileJcstressJava: this source set compiles against the main module's
// exported packages as plain classpath classes.
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
