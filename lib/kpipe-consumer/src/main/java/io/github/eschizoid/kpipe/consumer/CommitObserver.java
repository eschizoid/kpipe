package io.github.eschizoid.kpipe.consumer;

/// Hears the outcome of each offset commit [KafkaOffsetManager] makes: periodic, on revoke and at
/// close, to Kafka or to an [OffsetStore]. The consumer counts them in its metrics.
interface CommitObserver {
  /// An observer that ignores every outcome.
  CommitObserver NONE = new CommitObserver() {
    @Override
    public void committed() {}

    @Override
    public void failed() {}
  };

  /// A commit succeeded.
  void committed();

  /// A commit failed or did not finish in time.
  void failed();
}
