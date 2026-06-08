package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/// Pins the exception-handling contract on [DefaultHandle#startAndWrap]: if `consumer.start()`
/// throws, the consumer MUST be closed (so the `AutoCloseable` contract holds when the caller
/// never gets a Handle back), and if `close()` ALSO throws, the secondary exception must be
/// attached as a suppressed exception on the original rather than swallowed or rethrown.
@ExtendWith(MockitoExtension.class)
class DefaultHandleStartAndWrapTest {

  @Mock
  private KPipeConsumer<byte[]> consumer;

  @Test
  void happyPathStartsAndWrapsTheConsumer() {
    final var handle = DefaultHandle.startAndWrap(consumer);

    assertNotNull(handle);
    assertInstanceOf(DefaultHandle.class, handle);
    verify(consumer).start();
    verify(consumer, never()).close();
  }

  @Test
  void startFailureClosesConsumerAndRethrowsOriginal() {
    final var startFailure = new RuntimeException("start blew up");
    doThrow(startFailure).when(consumer).start();

    final var thrown = assertThrows(RuntimeException.class, () -> DefaultHandle.startAndWrap(consumer));

    // The ORIGINAL start() exception is what surfaces — not whatever close() did or didn't do.
    assertSame(startFailure, thrown);
    verify(consumer).start();
    verify(consumer).close();
    // No suppressed exception when close() succeeded.
    assertEquals(0, thrown.getSuppressed().length);
  }

  @Test
  void startFailureWithCloseFailureAttachesSuppressed() {
    final var startFailure = new RuntimeException("start blew up");
    final var closeFailure = new IllegalStateException("close blew up too");
    doThrow(startFailure).when(consumer).start();
    doThrow(closeFailure).when(consumer).close();

    final var thrown = assertThrows(RuntimeException.class, () -> DefaultHandle.startAndWrap(consumer));

    // The original start() failure is rethrown. The close() failure is attached as suppressed
    // — neither swallowed nor allowed to mask the real root cause.
    assertSame(startFailure, thrown);
    assertEquals(1, thrown.getSuppressed().length);
    assertSame(closeFailure, thrown.getSuppressed()[0]);
  }

  @Test
  void rejectsNullConsumer() {
    assertThrows(NullPointerException.class, () -> DefaultHandle.startAndWrap(null));
  }
}
