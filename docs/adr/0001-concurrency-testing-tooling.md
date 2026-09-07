# ADR 0001 — Concurrency testing moves to Fray

- **Status:** Accepted
- **Date:** 2026-09-07
- **Supersedes:** nothing (first ADR in this repo)

## Context

kpipe gates every PR on a 21-class [jcstress](https://github.com/openjdk/jcstress) suite covering the offset
frontier, remove-if-empty, the backpressure handshake, dispatcher counters, and the key-ordered worker handoff and
eviction tombstone. The suite is the reason the at-least-once claim is testable rather than asserted.

jcstress is dormant. Its last release is **0.16 (2023-02-27)** — the version we pin — and its last upstream commit is
**2025-06-19**. It is not archived, and those final commits were JDK 25 fixes (native-access and `sun.misc.Unsafe`
warnings), but **those fixes exist only in git; no release carries them**. That raises a fair question: should the
suite move to a maintained tool?

Two candidates were evaluated by experiment rather than reputation.

## Decision

**Migrate the suite to [Fray](https://github.com/cmu-pasta/fray) and retire jcstress.**

Fray explores schedules under a controlled scheduler and replays failures deterministically, rather than running racy
code many times and hoping the bad interleaving surfaces. Our own numbers are the argument: the eviction-tombstone
window was reached **78 times out of 23,427 stress runs — 0.33%**. That window is currently found by luck, on every
run, forever. A scheduler-controlling tool targets it.

Fray is also the only maintained option: v0.9.0 released 2026-07-17, with CI covering JDK 11/21/25 on Linux, Windows
and macOS. jcstress has had no release since 0.16 (2023-02-27) and its JDK 25 fixes exist only in git.

Lincheck was evaluated and rejected — see below; it cannot instrument Java 25 class files at all.

## Evidence

Each tool was given the same probe: a deliberately racy counter (`counter++` from two threads, non-atomic) that a
working concurrency checker must reject.

### Lincheck 2.39 — silently passes on Java 25

| JDK | Result |
| --- | --- |
| 21 | **Correctly fails**, printing the exact interleaving (both threads read 0, both write 1) |
| 25 | **Passes** — false green |

Cause: Lincheck instruments bytecode with a bundled ASM that cannot read class file **major version 69** (Java 25).

```
java.lang.IllegalArgumentException: Unsupported class file major version 69
    at org.objectweb.asm.ClassReader.<init>
    at org.jetbrains.kotlinx.lincheck.transformation.LincheckClassFileTransformer
```

The transformer logs `Unable to transform`, continues, and the test passes. Lincheck itself is actively maintained
(2.39, 2025-04-09), so this may lift with a future release.

### Fray 0.9.0 — cannot run on this project's development machine

Fray is the most actively maintained of the three (v0.9.0 released 2026-07-17). It ships a **native JVMTI agent**,
and its Gradle plugin enumerates the platforms that agent exists for:

```kotlin
val supportedOsArchitectures = listOf("linux-x8664", "windows-x8664", "macos-aarch64")
```

macOS on x86_64 is absent, which is this project's primary development machine. On an unsupported platform the plugin
prints `Fray JVMTI agent will not be added as a dependency` and the build proceeds — so the racy counter **passed**.
Published artifacts confirm the list: `fray-jvmti-linux-x8664` resolves; every macOS/x86 variant 404s.

Fray is **not** JDK-blocked the way Lincheck is: its own CI matrix runs JDK 11, 21, and 25 across
`ubuntu-latest`, `windows-latest`, and `macos-latest` (Apple Silicon). CI here is `ubuntu-latest` = `linux-x8664`,
which is supported on all three counts — the platform list, the published artifact, and Fray's tested JDK range.

The macOS x86_64 gap is real but not disqualifying. The suite's job is to gate CI, which runs `ubuntu-latest`;
jcstress was never part of a local dev loop either at 30-47 minutes per run. For local reproduction, Docker on
`linux/amd64` is a supported Fray platform on the same hardware.

The gap does create one hazard that must be engineered around: on an unsupported platform Fray skips its agent and
tests **pass**. See "Silent no-op is the failure mode to guard" below.

### Tools considered and not probed

- **Java PathFinder** — effectively pinned to old JDKs; impractical against a Java 25 target.
- **Error Prone `@GuardedBy`** — static lock-discipline checking. Complementary, not a replacement; worth adopting on
  its own merits since `KeyOrderedDispatcher` documents which monitor guards which field in prose only.
- **TLA+** — specification-level. A different layer of the verification story, not a jcstress substitute.

## Consequences

- The 21-class jcstress suite is ported to Fray and jcstress is removed, including its Gradle source sets and the
  dedicated CI job.
- **Accepted, deliberately: memory-model coverage is dropped.** Fray controls thread scheduling; it does not model
  hardware reordering. `CasPublicationJCStressTest` and the ARM/weakly-ordered campaign have no successor under this
  decision — the campaign item is deleted from the roadmap rather than deferred. The judgement is that kpipe's
  concurrency risk is overwhelmingly interleaving-shaped (lost enqueue, evict-vs-enqueue, remove-if-empty, worker
  handoff, offset frontier), and that those are exactly what Fray targets directly instead of by chance.
- CI keeps a dedicated concurrency job; expected to be substantially faster than the 30-35 minute stress baseline,
  though this is unmeasured until the pilot lands.
- Local runs on macOS x86_64 will not exercise Fray. Reproduction uses a `linux/amd64` container.

## Silent no-op is the failure mode to guard

Both candidates evaluated here **passed** a deliberately racy test when they could not instrument it. That is the
worst possible failure mode for a correctness gate: it converts an untested invariant into a green check.

The suite must therefore carry a permanent self-check that fails the build when Fray is not actually instrumenting,
rather than trusting the plugin's log line. A version bump, a JDK bump, or a new runner architecture can all silently
disable instrumentation, and none of them would otherwise turn CI red.

## Rejected: Lincheck

Kept on record so this is not re-evaluated from documentation alone. Lincheck is well maintained (2.39, 2025-04-09)
and produces the best failure output of the three, but it cannot read Java 25 class files, and it fails open.

## Revisit when

1. **Fray publishes a `macos-x8664` agent, or this machine moves to Apple Silicon.** Local runs become first-class and
   the container wrapper can go.
2. **A memory-model bug escapes to production.** That is the evidence that would reverse the accepted risk above; the
   response is to reintroduce a small jcstress remnant for JMM outcomes only, not to restore the full suite.
3. **Fray stops being maintained.** Re-run the probe against whatever the field offers then.

## Note on method

Both candidates were rejected-or-accepted on evidence produced in about twenty minutes of probing, and both would have
looked fine from their documentation. Any future concurrency-tool evaluation starts the same way: give the tool code
with a known race and confirm it **fails**. A tool that cannot fail is worse than no tool.
