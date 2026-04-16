plugins {
  `java-library`
  jacoco
  `maven-publish`
  signing
}

java {
  withSourcesJar()
  withJavadocJar()
  modularity.inferModulePath.set(true)
  toolchain {
    languageVersion = JavaLanguageVersion.of(25)
  }
}

repositories {
  mavenCentral()
}

dependencies {
  api(project(":lib:kpipe-producer"))

  // Kafka
  implementation(libs.kafkaClients)

  // DSL-JSON
  implementation(libs.dslJson)
  annotationProcessor(libs.dslJson)
  testAnnotationProcessor(libs.dslJson)

  // Avro
  implementation(libs.avro)

  // Testing
  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)

  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)

  testImplementation(libs.slf4jSimple)

  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainersJunitJupiter)
  testImplementation(libs.testcontainersKafka)
  testImplementation(libs.testcontainersPostgresql)
  testImplementation(libs.postgresql)
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

tasks.compileJava {
  doFirst {
    options.compilerArgs.addAll(listOf("--module-path", classpath.asPath))
    classpath = files()
  }
}

tasks.javadoc {
  options.modulePath = classpath.toList()
  classpath = files()
}

publishing {
  publications {
    create<MavenPublication>("maven") {
      groupId = "io.github.eschizoid"
      artifactId = "kpipe-consumer"
      from(components["java"])

      pom {
        name.set("kpipe-consumer")
        description.set("KPipe Consumer - Functional Kafka consumer with virtual threads")
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

val signingKey = System.getenv("JRELEASER_GPG_SECRET_KEY") ?: project.properties["signing.secretKey"]?.toString()
val signingPassword =
  System.getenv("JRELEASER_GPG_PASSPHRASE") ?: project.properties["signing.password"]?.toString()

signing {
  isRequired = signingKey != null && signingPassword != null

  afterEvaluate {
    sign(publishing.publications["maven"])
  }

  if (signingKey != null && signingPassword != null) useInMemoryPgpKeys(signingKey, signingPassword)
}
