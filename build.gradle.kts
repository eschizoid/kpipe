plugins {
  alias(libs.plugins.spotless)
  alias(libs.plugins.axion)
}

scmVersion {
  unshallowRepoOnCI.set(true)
  tag {
    prefix.set("v")
  }
  versionCreator("versionWithBranch")
  branchVersionCreator.set(
    mapOf(
      "main" to "simple",
    ),
  )
  val incrementType =
    when (project.findProperty("release.incrementer")?.toString()) {
      "patch" -> "incrementPatch"
      "minor" -> "incrementMinor"
      "major" -> "incrementMajor"
      else -> "incrementMinor"
    }
  versionIncrementer(incrementType)
  branchVersionIncrementer.set(
    mapOf(
      "feature/.*" to "incrementMinor",
      "bugfix/.*" to "incrementPatch",
    ),
  )
}

allprojects {
  apply(plugin = "com.diffplug.spotless")

  group = "io.github.eschizoid"
  version = rootProject.scmVersion.version

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
    maven { url = uri("https://packages.confluent.io/maven/") }
  }

  spotless {
    kotlinGradle {
      ktlint()
        .editorConfigOverride(
          mapOf(
            "indent_size" to 2,
            "continuation_indent_size" to 2,
          ),
        )
      target("*.gradle.kts")
      trimTrailingWhitespace()
      endWithNewline()
    }
  }
}

subprojects {
  apply(plugin = "com.diffplug.spotless")

  spotless {
    java {
      target("src/**/*.java")
      googleJavaFormat("1.35.0")
      toggleOffOn()
      importOrder()
      removeUnusedImports()
      trimTrailingWhitespace()
      endWithNewline()
      ratchetFrom("origin/main")
      prettier(
        mapOf(
          "prettier" to "3.8.1",
          "prettier-plugin-java" to "2.8.1",
        ),
      ).config(
        mapOf(
          "plugins" to listOf("prettier-plugin-java"),
          "parser" to "java",
          "tabWidth" to 2,
          "printWidth" to 120,
        ),
      )
    }
  }

  tasks.withType<JavaCompile> {
    options.release.set(25)
  }
}
