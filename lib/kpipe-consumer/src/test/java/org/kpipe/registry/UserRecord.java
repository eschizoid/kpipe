package org.kpipe.registry;

import com.dslplatform.json.CompiledJson;

@CompiledJson
public record UserRecord(String id, String name, String email) {}
