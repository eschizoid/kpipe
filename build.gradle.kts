import org.jreleaser.model.Active.ALWAYS

plugins {
    application
    idea
    jacoco
    `maven-publish`
    signing
    id("java")
    id("com.diffplug.spotless") version "7.0.3"
    id("com.github.johnrengelman.shadow") version "8.1.1"
    id("pl.allegro.tech.build.axion-release") version "1.18.7"
    id("org.jreleaser") version "1.17.0"
}

scmVersion {
    unshallowRepoOnCI.set(true)
    tag {
        prefix.set("v")
    }
}

group = "org.kpipe"
description = "KPipe - Functional Kafka Consumer"
version = scmVersion.version

repositories {
    mavenCentral()
}

application {
    mainClass.set("org.kpipe.App")
}

java {
    withSourcesJar()
    withJavadocJar()
    toolchain {
        languageVersion = JavaLanguageVersion.of(23)
    }
}

repositories {
    mavenCentral()
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

tasks.jacocoTestReport {
    reports {
        xml.required.set(true)
        csv.required.set(true)
        html.required.set(true)
    }
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
        ratchetFrom("origin/main")
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

jreleaser {
    project {
        description.set("Functional Kafka Consumer Library")
        authors.set(listOf("Mariano Gonzalez"))
        license.set("Apache-2.0")
        links {
            homepage.set("https://github.com/eschizoid/kpipe")
        }
        inceptionYear.set("2025")
        tags.set(listOf("kafka", "consumer", "functional", "java"))
    }

    signing {
        active.set(ALWAYS)
        armored.set(true)
    }

    deploy {
        maven {
            mavenCentral {
                create("sonatype") {
                    active.set(ALWAYS)
                    url.set("https://central.sonatype.com/api/v1/publisher")
                    stagingRepositories.add("target/staging-deploy")
                }
            }
        }
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifact(tasks.shadowJar)

            pom {
                name.set("KPipe")
                description.set("Functional Kafka Consumer Library")
                url.set("https://github.com/eschizoid/kpipe")

                licenses {
                    license {
                        name.set("Apache License 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }

                developers {
                    developer {
                        id.set("eschizoid")
                        name.set("Mariano Gonzalez")
                        email.set("mariano.gonzalez.mx@gmail.com")
                    }
                }

                scm {
                    connection.set("scm:git:git://github.com/eschizoid/kpipe.git")
                    developerConnection.set("scm:git:ssh://github.com/eschizoid/kpipe.git")
                    url.set("https://github.com/eschizoid/kpipe")
                }
            }
        }
    }
}

signing {
    val signingKey: String? by project
    val signingPassword: String? by project
    useInMemoryPgpKeys(signingKey, signingPassword)
    sign(publishing.publications["maven"])
}