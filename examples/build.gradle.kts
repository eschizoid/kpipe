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
    implementation(rootProject.libs.kafkaClients)
    implementation(rootProject.libs.slf4jSimple)

    testImplementation(rootProject.libs.junitJupiter)
    testImplementation(rootProject.libs.testcontainers)
    testImplementation(rootProject.libs.testcontainersJunitJupiter)
    testImplementation(rootProject.libs.testcontainersKafka)
    testImplementation(rootProject.libs.dslJson)
    testRuntimeOnly(rootProject.libs.junitPlatformLauncher)
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
