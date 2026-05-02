plugins {
  `java-platform`
  `maven-publish`
  signing
}

description = "KPipe BOM — pins all kpipe-* artifacts to matching versions"

dependencies {
  constraints {
    api(project(":lib:kpipe-metrics"))
    api(project(":lib:kpipe-metrics-otel"))
    api(project(":lib:kpipe-consumer"))
    api(project(":lib:kpipe-producer"))
    api(project(":lib:kpipe-format-json"))
    api(project(":lib:kpipe-format-avro"))
    api(project(":lib:kpipe-format-protobuf"))
  }
}

publishing {
  publications {
    create<MavenPublication>("maven") {
      groupId = "io.github.eschizoid"
      artifactId = "kpipe-bom"
      from(components["javaPlatform"])

      pom {
        name.set("kpipe-bom")
        description.set(
          "KPipe BOM — Maven Bill-of-Materials that pins all kpipe-* modules to matching versions",
        )
        url.set("https://github.com/eschizoid/kpipe")
        inceptionYear.set("2025")
        packaging = "pom"

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
