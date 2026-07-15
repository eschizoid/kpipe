plugins {
  `java-platform`
}

description = "KPipe BOM — Maven Bill-of-Materials that pins all kpipe-* modules to matching versions"

// Derive the constrained set from the :lib project graph instead of hand-listing modules. Every
// sibling :lib module except this BOM is a published artifact that must be pinned; sourcing from
// `parent.subprojects` keeps the BOM in lockstep with `publishAllLibModules` and the JReleaser
// deploy list (both already graph-sourced), so a newly added lib module can't silently drop out of
// the BOM — the drift that left tracing-otel, schema-registry-confluent, and format-protobuf-confluent
// unpinned through 1.17.0.
dependencies {
  constraints {
    publishedLibModules().forEach { api(project(it.path)) }
  }
}

// CI guard: fail the build if the BOM's constrained set ever diverges from the publishable :lib set.
// Redundant with the graph-sourced derivation above by construction, but it pins the invariant so a
// future refactor back to a hand-maintained list can't silently reintroduce the drift. Runs as part
// of `check` (hence `build`), which CI already invokes.
val verifyBomCoverage by tasks.registering {
  group = "verification"
  description = "Asserts the BOM constrains exactly the set of published :lib modules."
  doLast {
    val expected = publishedLibModules().map { it.name }.toSortedSet()
    val actual = configurations["api"].dependencyConstraints.map { it.name }.toSortedSet()
    check(actual == expected) {
      "BOM constraint set drifted from the publishable :lib modules.\n" +
        "  missing from BOM: ${expected - actual}\n" +
        "  extra in BOM:     ${actual - expected}"
    }
  }
}

tasks.named("check") { dependsOn(verifyBomCoverage) }

// The publishable :lib modules: every sibling under :lib except the BOM itself (a BOM never
// constrains itself). Same graph source as `publishAllLibModules` / the JReleaser deploy list.
fun publishedLibModules(): List<Project> = parent!!.subprojects
  .filter { it.name != "kpipe-bom" }
  .sortedBy { it.name }
