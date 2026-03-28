import org.gradle.api.artifacts.VersionCatalogsExtension
import org.jreleaser.model.Active.ALWAYS
import org.jreleaser.model.Active.NEVER

plugins {
    java
    `maven-publish`
    signing
    jacoco
    id("org.jreleaser") version "1.23.0"
}

val libsCatalog = rootProject.extensions.getByType<VersionCatalogsExtension>().named("libs")

description = "KPipe - Functional Kafka Consumer Library"

java {
    withSourcesJar()
    withJavadocJar()
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
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
    implementation(libsCatalog.findLibrary("kafkaClients").get())

    // DSL-JSON
    implementation(libsCatalog.findLibrary("dslJson").get())
    annotationProcessor(libsCatalog.findLibrary("dslJson").get())
    testAnnotationProcessor(libsCatalog.findLibrary("dslJson").get())

    // Avro
    implementation(libsCatalog.findLibrary("avro").get())

    // SLF4J API only
    implementation(libsCatalog.findLibrary("slf4jApi").get())

    // Testing
    testImplementation(platform(libsCatalog.findLibrary("junitBom").get()))
    testImplementation(libsCatalog.findLibrary("junitJupiter").get())
    testRuntimeOnly(libsCatalog.findLibrary("junitPlatformLauncher").get())

    testImplementation(libsCatalog.findLibrary("mockitoCore").get())
    testImplementation(libsCatalog.findLibrary("mockitoJunitJupiter").get())

    testImplementation(libsCatalog.findLibrary("slf4jSimple").get())

    testImplementation(libsCatalog.findLibrary("testcontainers").get())
    testImplementation(libsCatalog.findLibrary("testcontainersJunitJupiter").get())
    testImplementation(libsCatalog.findLibrary("testcontainersPostgresql").get())
    testImplementation(libsCatalog.findLibrary("postgresql").get())
}

tasks.test {
    useJUnitPlatform()

    if (project.hasProperty("excludeTests")) {
        val excludePattern = project.property("excludeTests").toString()
        exclude("**/${excludePattern.replace(".", "/")}.class")
    }

    minHeapSize = "7g"
    maxHeapSize = "7g"
    maxParallelForks = 1
    forkEvery = 200
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
            url = uri(layout.buildDirectory.dir("staging-deploy"))
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
        active.set(NEVER)
    }

    deploy {
        maven {
            mavenCentral {
                create("sonatype") {
                    active.set(ALWAYS)
                    url.set("https://central.sonatype.com/api/v1/publisher")
                    stagingRepository(layout.buildDirectory.dir("staging-deploy").get().asFile.absolutePath)
                    enabled.set(true)
                    sign.set(false)
                    maxRetries.set(10)
                }
            }
        }
    }

    release {
        github {
            enabled.set(true)
            overwrite.set(true)
            changelog {
                formatted.set(ALWAYS)
                preset.set("conventional-commits")
            }
        }
    }
}
