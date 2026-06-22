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
