import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.artifacts.VersionCatalogsExtension
import org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL

plugins {
  java
  id("com.gradleup.shadow") version "8.3.5" apply false
}

val libsCatalog = rootProject.extensions.getByType<VersionCatalogsExtension>().named("libs")

subprojects {
  apply(plugin = "java")
  apply(plugin = "com.gradleup.shadow")

  dependencies {
    add("implementation", project(":lib"))
    add("implementation", libsCatalog.findLibrary("kafkaClients").get())
    add("implementation", libsCatalog.findLibrary("slf4jSimple").get())

    add("testImplementation", libsCatalog.findLibrary("junitJupiter").get())
    add("testImplementation", libsCatalog.findLibrary("testcontainers").get())
    add("testImplementation", libsCatalog.findLibrary("testcontainersJunitJupiter").get())
    add("testImplementation", libsCatalog.findLibrary("testcontainersKafka").get())
    add("testImplementation", libsCatalog.findLibrary("dslJson").get())
    add("testRuntimeOnly", libsCatalog.findLibrary("junitPlatformLauncher").get())
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
