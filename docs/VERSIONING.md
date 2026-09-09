# Versioning and compatibility

What breaks, when, and how you find out. This page exists so you can decide whether to depend on KPipe without
reading its commit history.

## The short version

**Through 1.x, a minor release may remove or rename public API.** There is no deprecation cycle: an API that goes
is deleted in the same change that migrates its callers, and the release notes carry a migration table for every
break. Patch releases never break anything.

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

| Tier | What it covers | Stability |
| --- | --- | --- |
| **Public API** | `KPipe`, `Stream`, `Sink`, `Handle`, `MessageFormat`, `MessageSink`, `BatchSink`, `Result`, the `RegistryKey` / `MessageProcessorRegistry` pair, and the builders reachable from them | Covered by the rules above |
| **SPI** | `SchemaResolver`, `Tracer`, `ProtobufDescriptorCompiler`, `OffsetManager`, `ConsumerMetrics`, `ProducerMetrics` | Same rules, but implementing one means a new abstract method is a break for you and not for a caller |
| **Internal** | Anything package-private, anything under a package a module does not export, and every `*Dispatcher`, `*Controller` and `*Ledger` type | No guarantees. May change in any release |

A type being `public` for JPMS reasons does not make it public API — the module's `exports` clauses are the
boundary that counts.

## How a break reaches you

- **Release notes** carry a migration table: the removed form, the replacement, and the mechanical edit.
- **The PR that removes it** names the removal in its description and updates every caller in the repository,
  so there is always a worked example.
- **No compiler warning.** That is the cost of having no deprecation cycle, and it is the reason to read the notes
  for a minor upgrade rather than assuming a minor is safe.

## Support window

The newest minor on the current major receives fixes. Older minors do not get backports; upgrading forward is the
supported path. When 2.0 arrives, 1.x gets security fixes for six months.

## Java baseline

KPipe targets Java 25 and uses virtual threads throughout. A Java baseline increase is treated as a breaking change
and follows the same rules as an API removal.
