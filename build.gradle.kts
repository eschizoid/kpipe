plugins {
    id("java")
    id("com.diffplug.spotless") version "7.0.3"
}

group = "kpipe"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
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

