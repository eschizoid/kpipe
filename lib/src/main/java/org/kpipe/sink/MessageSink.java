package org.kpipe.sink;

import java.util.function.Consumer;

/// Functional interface representing a destination for processed messages.
///
/// @param <T> The type of the processed object.
@FunctionalInterface
public interface MessageSink<T> extends Consumer<T> {}
