plugins {
    id("java")
    id("com.diffplug.spotless") version "6.21.0"
}

group = "org.example"
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
    // Kafka dependencies
    implementation("org.apache.kafka:kafka-clients:3.9.0")

    // DSL-JSON library
    implementation("com.dslplatform:dsl-json:2.0.2")

    // JUnit 5 for testing
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    // Mockito dependencies with ByteBuddy
    testImplementation("org.mockito:mockito-core:5.17.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.17.0")
    testImplementation("net.bytebuddy:byte-buddy:1.17.5")
    testImplementation("net.bytebuddy:byte-buddy-agent:1.17.5")
}

tasks.test {
    useJUnitPlatform()
}

spotless {
    java {
        target("src/**/*.java") // Target all Java files in the project
        googleJavaFormat("1.17.0") // Use Google Java Format
    }
}

