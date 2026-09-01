<!-- Keep a Changelog guide -> https://keepachangelog.com -->

# Kafka Premature Offset Commit Companion Changelog

## [Unreleased]

## [0.1.0]

### Added

- Warning on a Kafka consumer poll-loop that dispatches async work
  without waiting for it to finish before committing offsets
  (explicitly or via auto-commit) -- a real, silent message-loss risk
  if the consumer crashes mid-flight.
- Correlates three independent signals in the same loop body: async
  dispatch, absence of a synchronous wait, and the commit shape
  (explicit vs. default auto-commit).

[Unreleased]: https://github.com/GapHunterLabs/kafka-premature-offset-commit-companion/compare/0.1.0...HEAD
[0.1.0]: https://github.com/GapHunterLabs/kafka-premature-offset-commit-companion/commits/0.1.0
