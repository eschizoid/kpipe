plugins {
    application
    id("java")
    id("com.diffplug.spotless") version "7.0.3"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "kpipe"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

application {
    mainClass.set("com.example.kafka.KafkaConsumerApp")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(23)
    }
}

dependencies {
    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.9.0")

    // DSL-JSON
    implementation("com.dslplatform:dsl-json:2.0.2")

    // SLF4J
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("org.slf4j:slf4j-simple:2.0.9")

    // JUnit 5
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    // Mockito
    testImplementation("org.mockito:mockito-core:5.17.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.17.0")
}

tasks.test {
    useJUnitPlatform()
}

// Disable the standard jar task
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

tasks.named("startScripts") {
    dependsOn(tasks.shadowJar)
}

tasks.named("distTar") {
    dependsOn(tasks.shadowJar)
}

tasks.named("distZip") {
    dependsOn(tasks.shadowJar)
}

tasks.build {
    dependsOn(tasks.shadowJar)
}

spotless {
    java {
        target("src/**/*.java")
        googleJavaFormat("1.26.0")
        toggleOffOn()
        importOrder()
        removeUnusedImports()
        prettier(
            mapOf(
                "prettier" to "2.8.8",
                "prettier-plugin-java" to "2.1.0"
            )
        ).config(
            mapOf(
                "parser" to "java",
                "tabWidth" to 2,
                "printWidth" to 120
            )
        )
    }
}