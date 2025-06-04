import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version "8.1.1" apply false
}

subprojects {
    apply(plugin = "application")
    apply(plugin = "com.github.johnrengelman.shadow")

    dependencies {
        "implementation"(project(":lib"))
        "implementation"("org.apache.kafka:kafka-clients:3.9.0")
        "implementation"("org.slf4j:slf4j-simple:2.0.9")
    }

    tasks.withType<JavaCompile> {
        options.release.set(24)
    }

    tasks.named("jar") {
        enabled = false
    }

    tasks.named<ShadowJar>("shadowJar") {
        archiveBaseName.set("kpipe-${project.name}")
        archiveClassifier.set("")
        archiveVersion.set(version.toString())
        manifest {
            attributes(
                mapOf(
                    "Main-Class" to application.mainClass.get(),
                    "Implementation-Title" to project.name,
                    "Implementation-Version" to project.version
                )
            )
        }
        mergeServiceFiles()
    }

    tasks.named("build") {
        dependsOn("shadowJar")
    }
}
