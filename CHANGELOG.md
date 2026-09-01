# Changelog

All notable changes to Enterprise Crawler Framework are documented in this
file.

The project follows Semantic Versioning.

## [1.0.1] - 2026-09-01

### Changed

- Expanded the public README into a developer-facing product and onboarding
  surface.
- Made the PyPI installation path the primary installation guidance.
- Added a copy-paste quickstart using the supported top-level public API.
- Documented the existing runtime, HTTP, storage, processing, plugin, event,
  worker, retry, dead-letter, and CLI capabilities.
- Added explicit project links for source, issues, releases, changelog, and
  security information.
- Added package keywords and supported-Python classifiers to improve package
  discovery metadata.
- Refined the package description to describe ECF as production-oriented
  infrastructure for Python crawlers and data-collection bots.

### Compatibility

- No runtime behavior was intentionally changed.
- No top-level public API symbol was added, removed, or renamed.
- No CLI command was added, removed, or renamed.
- No plugin contract was changed.
- No event, worker, retry, queue, lease, or dead-letter semantics were changed.
- `1.0.1` is a backward-compatible documentation, onboarding, and package
  metadata patch.

## [1.0.0] - 2026-08-20

### Added

- Stable `BaseBot` lifecycle contract.
- Top-level `Crawler` runtime facade.
- Public `ExecutionResult` and `ExecutionStatus` contracts.
- Cooperative shutdown support.
- Runtime state and execution snapshots.
- HTTP client with retry and circuit-breaker behavior.
- Session management.
- Streaming downloader with size and SHA-256 validation.
- Opt-in local storage.
- Atomic file writing.
- SQLite-backed local state storage.
- Configuration models and loading helpers.
- JSON processing.
- XML processing.
- CSV processing.
- HTML processing.
- Feed processing.
- PDF processing primitives.
- Composable processing pipeline.
- Plugin registry.
- Plugin manager.
- Plugin loader.
- Metadata-only plugin discovery.
- Python entry-point plugin discovery.
- Plugin autoload composition.
- In-memory event queue.
- Durable SQLite event queue.
- Claim-token ownership.
- Queue leases and expired-lease recovery.
- Event dispatcher.
- Event worker.
- Retry classification policy.
- Exponential retry backoff.
- Durable scheduled retries.
- Configurable retry delay caps.
- Injectable retry jitter.
- In-memory dead-letter queue.
- Durable SQLite dead-letter queue.
- CLI `version` command.
- CLI `doctor` command.
- CLI plugin discovery commands.
- Top-level public API for the primary bot workflow.
- Runnable `examples/basic_bot/hello_bot.py` quickstart.
- Wheel and source-distribution build support.
- Dynamic package versioning from `enterprise_crawler.version.__version__`.
- Release artifact clean-install smoke testing.
- GitHub Actions test, lint, and release gates.

### Guarantees

- `BaseBot.run()` is framework-owned and cannot be overridden by subclasses.
- Injected runtime dependencies are not automatically owned by consumers.
- Storage and plugins are opt-in.
- Plugin discovery does not import third-party plugin code.
- Event acknowledgement and negative acknowledgement require valid ownership.
- Stale queue claim tokens cannot finalize recovered messages.
- Lease expiration and retry scheduling remain separate concepts.
- Scheduled retries survive SQLite reopen and process restart.
- Retry waiting does not increment delivery count.
- Dead-letter transfer is fail-closed.
- Retry exhaustion cannot create an infinite immediate requeue loop.
- Default retry jitter is disabled and preserves deterministic behavior.
- Wheel and source-distribution metadata use the same runtime version source.

### Verification

The final pre-release local regression baseline for 1.0.0 is:

- 1629 unit tests passed.
- 103 integration tests passed.
- 2 integration tests skipped.
- 1732 total tests passed.
- 0 failures.
- 0 errors.

Release artifacts were additionally verified through clean wheel and source
distribution installations outside the repository.