import org.jreleaser.model.Active.ALWAYS
import org.jreleaser.model.Active.NEVER

plugins {
  alias(libs.plugins.jreleaser)
}

description = "KPipe - Lightweight Kafka processing library for modern Java"

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
          stagingRepository(
            project(":lib:kpipe-bom").layout.buildDirectory
              .dir("staging-deploy")
              .get()
              .asFile.absolutePath,
          )
          stagingRepository(
            project(":lib:kpipe-core").layout.buildDirectory
              .dir("staging-deploy")
              .get()
              .asFile.absolutePath,
          )
          stagingRepository(
            project(":lib:kpipe-metrics").layout.buildDirectory
              .dir("staging-deploy")
              .get()
              .asFile.absolutePath,
          )
          stagingRepository(
            project(":lib:kpipe-metrics-otel").layout.buildDirectory
              .dir("staging-deploy")
              .get()
              .asFile.absolutePath,
          )
          stagingRepository(
            project(":lib:kpipe-producer").layout.buildDirectory
              .dir("staging-deploy")
              .get()
              .asFile.absolutePath,
          )
          stagingRepository(
            project(":lib:kpipe-consumer").layout.buildDirectory
              .dir("staging-deploy")
              .get()
              .asFile.absolutePath,
          )
          stagingRepository(
            project(":lib:kpipe-format-json").layout.buildDirectory
              .dir("staging-deploy")
              .get()
              .asFile.absolutePath,
          )
          stagingRepository(
            project(":lib:kpipe-format-avro").layout.buildDirectory
              .dir("staging-deploy")
              .get()
              .asFile.absolutePath,
          )
          stagingRepository(
            project(":lib:kpipe-format-protobuf").layout.buildDirectory
              .dir("staging-deploy")
              .get()
              .asFile.absolutePath,
          )
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

  files {
    artifact {
      path.set(project(":lib:kpipe-bom").layout.buildDirectory.file("libs/kpipe-bom-{{projectVersion}}.pom"))
    }
    artifact {
      path.set(project(":lib:kpipe-core").layout.buildDirectory.file("libs/kpipe-core-{{projectVersion}}.jar"))
    }
    artifact {
      path.set(project(":lib:kpipe-metrics").layout.buildDirectory.file("libs/kpipe-metrics-{{projectVersion}}.jar"))
    }
    artifact {
      path.set(
        project(":lib:kpipe-metrics-otel").layout.buildDirectory.file("libs/kpipe-metrics-otel-{{projectVersion}}.jar"),
      )
    }
    artifact {
      path.set(project(":lib:kpipe-producer").layout.buildDirectory.file("libs/kpipe-producer-{{projectVersion}}.jar"))
    }
    artifact {
      path.set(project(":lib:kpipe-consumer").layout.buildDirectory.file("libs/kpipe-consumer-{{projectVersion}}.jar"))
    }
    artifact {
      path.set(
        project(":lib:kpipe-format-json").layout.buildDirectory.file("libs/kpipe-format-json-{{projectVersion}}.jar"),
      )
    }
    artifact {
      path.set(
        project(":lib:kpipe-format-avro").layout.buildDirectory.file("libs/kpipe-format-avro-{{projectVersion}}.jar"),
      )
    }
    artifact {
      path.set(
        project(":lib:kpipe-format-protobuf")
          .layout.buildDirectory
          .file("libs/kpipe-format-protobuf-{{projectVersion}}.jar"),
      )
    }
  }
}
