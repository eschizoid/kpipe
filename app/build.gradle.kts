import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL

plugins {
    java
    id("com.gradleup.shadow") version "8.3.5" apply false
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "com.gradleup.shadow")

    dependencies {
        "implementation"(project(":lib"))
        "implementation"("org.apache.kafka:kafka-clients:3.9.0")
        "implementation"("org.slf4j:slf4j-simple:2.0.9")

        "testImplementation"("org.junit.jupiter:junit-jupiter:5.10.0")
        "testImplementation"("org.testcontainers:testcontainers:2.0.3")
        "testImplementation"("org.testcontainers:testcontainers-junit-jupiter:2.0.3")
        testImplementation("org.testcontainers:testcontainers-kafka:2.0.3")
        "testImplementation"("com.dslplatform:dsl-json:2.0.2")
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
