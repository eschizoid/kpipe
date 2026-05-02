package org.kpipe.format.json;

import com.dslplatform.json.CompiledJson;

@CompiledJson
public record UserRecord(String id, String name, String email) {}
