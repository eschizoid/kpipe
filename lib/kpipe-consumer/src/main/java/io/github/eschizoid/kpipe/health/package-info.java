/// Optional HTTP health endpoint.
///
/// [HttpHealthServer] serves a consumer's [io.github.eschizoid.kpipe.consumer.HealthSnapshot]
/// over `jdk.httpserver`, for Kubernetes, Nomad, or any orchestrator that polls an HTTP endpoint.
/// Wire it with `HttpHealthServer.fromEnv(handle::health, "app")`; the `HEALTH_*` environment
/// variables read by [io.github.eschizoid.kpipe.consumer.config.HealthConfig] configure or
/// disable it.
package io.github.eschizoid.kpipe.health;
