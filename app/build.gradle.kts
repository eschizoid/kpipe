import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL

plugins {
    java
    id("com.gradleup.shadow") version "8.3.5" apply false
}

val kafkaVersion = "3.9.0"
val slf4jVersion = "2.0.9"
val junitVersion = "5.10.0"
val testcontainersVersion = "2.0.3"
val dslJsonVersion = "2.0.2"

subprojects {
    apply(plugin = "java")
    apply(plugin = "com.gradleup.shadow")

    dependencies {
        "implementation"(project(":lib"))
        "implementation"("org.apache.kafka:kafka-clients:$kafkaVersion")
        "implementation"("org.slf4j:slf4j-simple:$slf4jVersion")

        "testImplementation"("org.junit.jupiter:junit-jupiter:$junitVersion")
        "testImplementation"("org.testcontainers:testcontainers:$testcontainersVersion")
        "testImplementation"("org.testcontainers:testcontainers-junit-jupiter:$testcontainersVersion")
        "testImplementation"("org.testcontainers:testcontainers-kafka:$testcontainersVersion")
        "testImplementation"("com.dslplatform:dsl-json:$dslJsonVersion")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
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
