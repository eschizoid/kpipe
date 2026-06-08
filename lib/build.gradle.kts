import org.jreleaser.model.Active.ALWAYS
import org.jreleaser.model.Active.NEVER

plugins {
  alias(libs.plugins.jreleaser)
}

description = "KPipe - Lightweight Kafka processing library for modern Java"

subprojects {
  apply(plugin = "maven-publish")
  apply(plugin = "signing")

  group = rootProject.group
  version = rootProject.version

  afterEvaluate {
    extensions.configure<PublishingExtension> {
      publications {
        create<MavenPublication>("maven") {
          groupId = "io.github.eschizoid"
          artifactId = project.name

          // BOM publishes the platform component; everything else publishes the java component.
          val component = if (plugins.hasPlugin("java-platform")) {
            components["javaPlatform"]
          } else {
            components["java"]
          }
          from(component)

          pom {
            name.set(project.name)
            description.set(
              project.description ?: "KPipe ${project.name} module",
            )
            url.set("https://github.com/eschizoid/kpipe")
            inceptionYear.set("2025")
            if (project.name == "kpipe-bom") packaging = "pom"

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

    val signingKey =
      System.getenv("JRELEASER_GPG_SECRET_KEY") ?: project.properties["signing.secretKey"]?.toString()
    val signingPassword =
      System.getenv("JRELEASER_GPG_PASSPHRASE") ?: project.properties["signing.password"]?.toString()

    extensions.configure<SigningExtension> {
      isRequired = signingKey != null && signingPassword != null
      sign(extensions.getByType<PublishingExtension>().publications["maven"])
      if (signingKey != null && signingPassword != null) useInMemoryPgpKeys(signingKey, signingPassword)
    }
  }
}

// Aggregator that publishes every :lib subproject (each applies maven-publish in the
// `subprojects {}` block above) to its local staging-deploy directory in one call. Sourcing
// the dependency set from the project graph keeps it in lockstep with the JReleaser deploy
// list below, so the release workflow can publish all lib modules without hand-maintaining
// a parallel module list in release.yaml.
tasks.register("publishAllLibModules") {
  group = "publishing"
  description = "Publishes every :lib:* module to its local staging-deploy directory."
  dependsOn(subprojects.map { "${it.path}:publish" })
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

  // Signing is delegated to Gradle's `signing` plugin (configured per-module via the `subprojects {}`
  // block above). JReleaser only handles deployment, so its signing stage is intentionally disabled.
  signing {
    active.set(NEVER)
  }

  deploy {
    maven {
      mavenCentral {
        create("sonatype") {
          active.set(ALWAYS)
          url.set("https://central.sonatype.com/api/v1/publisher")
          // Iterate every `:lib` subproject (which all apply maven-publish in `subprojects {}`
          // above) so adding a new lib module automatically participates in the release. The
          // previous hand-maintained list silently dropped kpipe-tracing-otel and
          // kpipe-schema-registry-confluent through v1.16.0; sourcing the list from the actual
          // project graph removes that failure mode entirely.
          subprojects.forEach { sub ->
            stagingRepository(
              sub.layout.buildDirectory
                .dir("staging-deploy")
                .get()
                .asFile.absolutePath,
            )
          }
          enabled.set(true)
          sign.set(false)
          maxRetries.set(60)
          retryDelay.set(60)
          extraProperties.put("retryOnAlreadyDeployed", true)
        }
      }
    }
  }

  release {
    github {
      enabled.set(true)
      overwrite.set(true)
      draft.set(false)
      prerelease {
        enabled.set(false)
      }
      changelog {
        formatted.set(ALWAYS)
        preset.set("conventional-commits")
      }
    }
  }
}
