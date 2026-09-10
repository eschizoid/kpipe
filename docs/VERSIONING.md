# Versioning and compatibility

What breaks, when, and how you find out. This page exists so you can decide whether to depend on KPipe without
reading its commit history.

## The short version

**Through 1.x, a minor release may remove or rename public API.** There is no deprecation cycle: an API that goes
is deleted in the same change that migrates its callers. Patch releases never break anything.

**From 2.0 onward, KPipe follows strict semantic versioning.** Breaking changes land only in majors, and anything
removed is deprecated for at least one minor first.

If that first rule is unacceptable for your project, pin an exact version and upgrade deliberately. The BOM makes
that a one-line change.

## Why it works this way today

Deprecation cycles have real costs: they enlarge the surface every future reader has to understand, they train
people to ignore warnings, and the promised removal is often never made, so the deprecated form outlives the thing
that replaced it. Deleting in one step keeps the API the size of the idea.

That trade favours the codebase over its users, which is the right way round while the API is still finding its
shape and wrong once people depend on it. 2.0 is where it flips.

## What counts as public API

The tiers are defined by a rule rather than a list, because an enumeration goes stale the moment a type is added.

**Public API** — any `public` type in an exported package that is not covered by the two rules below. Reaching it
from the documented paths (`KPipe` → `Stream` → `Sink` → `Handle`, `KPipeConsumer.builder()`, the format and sink
types they accept) is what makes it public, and that includes the types those signatures mention:
`CircuitBreakerController` and `BackpressureController` are parameters to `Stream` methods, so they are public API,
as are `BatchPolicy`, `BatchResult` and `ProcessingMode`. The published `kpipe-test` module is public API too —
user test code compiles against it.

**SPI** — a public interface users are expected to *implement*: `MessageFormat`, `MessageSink`, `BatchSink`,
`SchemaResolver`, `Tracer`, `ProtobufDescriptorCompiler`, `OffsetManager`, `ConsumerMetrics`,
`ProducerMetrics`, `KPipeMetricsReporter`.
Same stability rules, with one asymmetry worth knowing: adding a method to one of these is a breaking change for
an implementer and invisible to a caller.

**Internal** — anything **package-private**, plus a small set of types that are public only because
Java has no friend-module visibility. Within a single module, package-private is the boundary that
operates: every package in every JPMS module here is exported, with no qualified exports anywhere,
so nothing is hidden that way. `KeyOrderedDispatcher`, `ParallelDispatcher`, `SequentialDispatcher`,
`OffsetLedger`, `RecordProcessor`, `ConsumerHealthController`, `BatchPipelineWrapper` and
`PendingOffsetSet` are all package-private and carry no guarantees.

A helper shared *between* KPipe modules cannot be package-private, so it is forced public and
exported while remaining internal. `RegistryFunctions`, `ConsoleSinkSupport`, `ConfluentEnvelope`
and `WireDiagnostics` are those, and `KafkaOffsetManager.getPartitionState` is a test observation
point with no production caller. None of them are user surface. The shape does not identify them —
`Operators` and `ConsumerMetricKeys` look identical and are genuine API — so this list is the
answer, not a pattern to apply.

Note the name is never the rule: `CircuitBreakerController` and `BackpressureController` are public
API despite the suffix, because `Stream` methods take them as parameters.

## How a break reaches you

- **The PR that removes it** carries the migration detail — the removed form, the replacement, and
  the mechanical edit — and updates every caller in the repository, so there is always a worked
  example. The same detail is in the commit message, sometimes as a table and otherwise as a bullet
  list.
- **Release notes are generated from commit subjects**, so a break usually appears there as an
  ordinary line in a `## Changes` group rather than as a migration guide. Some releases add a
  hand-written breaking-change section above the generated changelog — v1.17.0 did — but that is
  not automatic and should not be relied on. Follow the commit.
- **No compiler warning.** That is the cost of having no deprecation cycle, and it is the reason to
  read the release diff for a minor upgrade rather than assuming a minor is safe. Where a commit
  subject carries the conventional-commit `!` marker (`refactor(consumer)!:`), that is reliable
  evidence of a break; its absence is not evidence of safety, because the marker is not applied
  consistently.

## Support window

The newest minor on the current major receives fixes. Older minors do not get backports — no maintenance branches
exist and none have ever been cut, so upgrading forward is the only supported path.

## Java baseline

KPipe targets Java 25 and uses virtual threads throughout. A Java baseline increase is treated as a breaking change
and follows the same rules as an API removal.
