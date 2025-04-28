plugins {
    id("com.diffplug.spotless") version "7.0.3"
    id("pl.allegro.tech.build.axion-release") version "1.18.7"
}

scmVersion {
    unshallowRepoOnCI.set(true)
    tag {
        prefix.set("v")
    }
}

allprojects {
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
    }
}

subprojects {
    apply(plugin = "com.diffplug.spotless")

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
                    "prettier" to "2.8.8", "prettier-plugin-java" to "2.1.0"
                )
            ).config(
                mapOf(
                    "parser" to "java", "tabWidth" to 2, "printWidth" to 120
                )
            )
        }
    }

    tasks.withType<JavaCompile> {
        options.release.set(24)
    }
}
