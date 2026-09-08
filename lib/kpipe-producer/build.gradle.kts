import org.pastalab.fray.gradle.FrayExtension

plugins {
  `java-library`
  jacoco
  alias(libs.plugins.fray)
}

description = "KPipe Producer - Functional Kafka producer wrapper for KPipe"

dependencies {
  api(project(":lib:kpipe-core"))
  api(project(":lib:kpipe-metrics"))
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
  frayTestImplementation(project(":lib:kpipe-metrics"))
  "frayTestRuntimeOnly"(libs.junitPlatformLauncher)
}

// Same rationale as compileJcstressJava: compiled against the main module's exported packages as
// plain classpath classes.
tasks.named<JavaCompile>("compileFrayTestJava") {
  modularity.inferModulePath.set(false)
}

// On an unsupported OS/arch the plugin leaves this as a plain Test run rather than failing, so
// every @FrayTest reports as skipped and the build reads green. FrayInstrumentationGuardTest in
// this source set is what turns that state red.
tasks.register<Test>("frayTest") {
  group = "verification"
  description = "Runs the Fray controlled-concurrency suite for kpipe-producer."
  testClassesDirs = frayTest.output.classesDirs
  classpath = frayTest.runtimeClasspath
  // Fray forks one JVM and drives scheduling itself; parallel forks would only fight over cores.
  maxParallelForks = 1
  testLogging { showStandardStreams = true }
}

configure<FrayExtension> {
  testTask = "frayTest"
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
  description = "Runs the jcstress concurrency harness for kpipe-producer."
  classpath = jcstress.runtimeClasspath
  mainClass.set("org.openjdk.jcstress.Main")
  jvmArgs("-Djdk.attach.allowAttachSelf=true")
  // Keep jcstress scratch files (results .bin.gz, HTML report) under build/ instead of the module dir.
  val outDir = layout.buildDirectory.dir("jcstress").get().asFile
  doFirst { outDir.mkdirs() }
  workingDir = outDir
  // -t matches every jcstress test in the producer package by its shared JCStressTest suffix.
  args("-t", "JCStressTest", "-iters", "1", "-time", "50", "-f", "1", "-v", "-r", "results")
}

tasks.test {
  minHeapSize = "512m"
  maxHeapSize = "2g"
  maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(2)
}
