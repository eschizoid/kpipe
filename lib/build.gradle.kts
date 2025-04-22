import org.jreleaser.model.Active.ALWAYS
import java.security.MessageDigest

plugins {
    java
    `maven-publish`
    signing
    jacoco
    id("org.jreleaser") version "1.17.0"
}

description = "KPipe - Functional Kafka Consumer Library"

java {
    withSourcesJar()
    withJavadocJar()
    toolchain {
        languageVersion = JavaLanguageVersion.of(23)
    }
}

repositories {
    mavenCentral()
    maven {
        credentials {
            username = System.getenv("JRELEASER_MAVENCENTRAL_SONATYPE_USERNAME")
                ?: project.properties["mavencentralSonatypeUsername"]?.toString()
            password = System.getenv("JRELEASER_MAVENCENTRAL_SONATYPE_PASSWORD")
                ?: project.properties["mavencentralSonatypePassword"]?.toString()
        }
        url = uri("https://central.sonatype.com/")
    }
}

dependencies {
    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.9.0")

    // DSL-JSON
    implementation("com.dslplatform:dsl-json:2.0.2")

    // SLF4J API only
    implementation("org.slf4j:slf4j-api:2.0.9")

    // Testing
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-core:5.17.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.17.0")
}

tasks.test {
    useJUnitPlatform()
}

tasks.jacocoTestReport {
    reports {
        csv.required.set(true)
        xml.required.set(true)
        html.required.set(true)
    }
}

afterEvaluate {
    tasks.withType<Jar>().configureEach {
        archiveBaseName.set("kpipe")
    }
}

signing {
    afterEvaluate {
        sign(publishing.publications["maven"])
    }

    val signingKey = System.getenv("JRELEASER_GPG_SECRET_KEY") ?: project.properties["signing.secretKey"]?.toString()
    val signingPassword =
        System.getenv("JRELEASER_GPG_PASSPHRASE") ?: project.properties["signing.password"]?.toString()

    if (signingKey != null && signingPassword != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "io.github.eschizoid"
            artifactId = "kpipe"
            from(components["java"])

            pom {
                name.set("kpipe")
                description.set("Functional Kafka Consumer Library")
                url.set("https://github.com/eschizoid/kpipe")
                inceptionYear.set("2025")

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

    repositories {
        maven {
            url = uri("lib/build/staging-deploy")
        }
    }
}

jreleaser {

    gitRootSearch.set(true)

    project {
        name.set("kpipe")
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
                    stagingRepository("lib/build/staging-deploy")
                    enabled.set(true)
                }
            }
        }
    }

    release {
        github {
            enabled.set(true)
            overwrite.set(false)
        }
    }
}
