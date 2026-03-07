import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

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
