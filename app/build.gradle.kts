plugins {
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

description = "KPipe - Kafka Consumer Application"

application {
    mainClass.set("org.kpipe.App")
}

dependencies {
    implementation(project(":lib"))
    implementation("org.slf4j:slf4j-simple:2.0.9")
}

tasks.jar {
    enabled = false
}

tasks.shadowJar {
    archiveBaseName.set("kpipe")
    archiveClassifier.set("")
    archiveVersion.set(version.toString())
    manifest {
        attributes(
            "Main-Class" to application.mainClass.get(),
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version
        )
    }
    mergeServiceFiles()
}

tasks.build {
    dependsOn(tasks.shadowJar)
}