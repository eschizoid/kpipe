/// Optional HTTP health / readiness endpoint.
///
/// [HttpHealthServer] exposes liveness and readiness probes over `jdk.httpserver`, suitable for
/// Kubernetes, Nomad, or any orchestrator that polls an HTTP endpoint. Wire-up is opt-in and
/// driven by [org.kpipe.consumer.config.HealthConfig]; if you do not need probes, you pay
/// nothing.
package org.kpipe.health;
