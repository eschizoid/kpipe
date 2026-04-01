import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL

plugins {
  java
  alias(libs.plugins.shadow) apply false
}

subprojects {
  apply(plugin = "java")
  apply(plugin = "com.gradleup.shadow")

  dependencies {
    implementation(project(":lib"))
    implementation(libs.kafkaClients)
    implementation(libs.slf4jSimple)

    testImplementation(libs.junitJupiter)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainersJunitJupiter)
    testImplementation(libs.testcontainersKafka)
    testImplementation(libs.dslJson)
    testRuntimeOnly(libs.junitPlatformLauncher)
  }

  tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
      showStandardStreams = true
      showExceptions = true
      exceptionFormat = FULL
      events("passed", "skipped", "failed")
    }
  }

  tasks.withType<JavaCompile> {
    options.release.set(25)
  }

  tasks.named("jar") {
    enabled = false
  }

  tasks.named<ShadowJar>("shadowJar") {
    archiveBaseName.set("kpipe-${project.name}")
    archiveClassifier.set("")
    archiveVersion.set(version.toString())
    mergeServiceFiles()
  }

  tasks.named("build") {
    dependsOn("shadowJar")
  }
}
