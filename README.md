# Enterprise Crawler Framework

[![PyPI version](https://img.shields.io/pypi/v/enterprise-crawler-framework)](https://pypi.org/project/enterprise-crawler-framework/)
[![Python versions](https://img.shields.io/pypi/pyversions/enterprise-crawler-framework)](https://pypi.org/project/enterprise-crawler-framework/)
[![Tests](https://github.com/canerenaltungul/enterprise-crawler-framework/actions/workflows/tests.yml/badge.svg)](https://github.com/canerenaltungul/enterprise-crawler-framework/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/pypi/l/enterprise-crawler-framework)](https://github.com/canerenaltungul/enterprise-crawler-framework/blob/main/LICENSE)

**The infrastructure around your crawlers.**

Enterprise Crawler Framework (ECF) is a production-oriented Python framework
for building reliable, reusable data-collection bots and crawler runtimes.

You write the source-specific crawler logic. ECF provides the reusable
infrastructure around it: lifecycle management, HTTP collection, retries,
state and storage, processing primitives, plugins, event-driven workloads,
durable queues, worker coordination, and explicit failure semantics.

ECF is not a hosted scraping service, proxy network, browser cloud, or
application-specific crawler. It is a framework for developers who want to
build and operate their own data-collection systems.

## Installation

Install the latest release from PyPI:

```bash
python -m pip install enterprise-crawler-framework
```

To install a specific release:

```bash
python -m pip install enterprise-crawler-framework==1.0.1
```

Requirements:

- Python 3.11 or newer
- `requests >= 2.31`

## Quickstart

The primary workflow uses the framework's small top-level public API:

```python
from enterprise_crawler import BaseBot, Crawler


class HelloBot(BaseBot):
    def execute(self) -> None:
        print("Hello from Enterprise Crawler Framework!")
        self.mark_record_processed()


with HelloBot(bot_name="hello-bot") as bot:
    crawler = Crawler(bot)
    result = crawler.run()

print(f"status={result.status.value}")
print(f"records_processed={result.records_processed}")
```

Expected output:

```text
Hello from Enterprise Crawler Framework!
status=completed
records_processed=1
```

The runnable repository example is available at:

```text
examples/basic_bot/hello_bot.py
```

From the repository root:

```bash
python examples/basic_bot/hello_bot.py
```

## Why ECF?

A basic crawler can start with a few lines of HTTP code.

Production data collection usually needs much more:

```text
request handling
retries
timeouts
lifecycle
shutdown
state
idempotency
storage
processing
plugins
event delivery
worker coordination
retry scheduling
dead-letter handling
failure semantics
```

Those concerns are repeatedly rebuilt around source-specific crawler logic.

ECF provides a reusable foundation for them while keeping application and
domain rules outside the framework core.

## Core capabilities

### Runtime

- `BaseBot` lifecycle foundation
- `Crawler` execution facade
- canonical `ExecutionResult`
- canonical `ExecutionStatus`
- cooperative shutdown
- runtime counters and metadata
- explicit resource ownership
- dependency injection

### HTTP collection

- managed HTTP sessions
- configurable timeouts
- retry support
- backoff behavior
- TLS verification
- connection pooling
- streaming downloads
- download size and SHA-256 validation
- circuit-breaker behavior

### Storage and state

Storage is opt-in.

Available primitives include:

- local storage
- atomic file writing
- SQLite-backed local state
- record-level idempotency support

Creating a simple bot does not automatically create storage directories,
SQLite databases, or plugin state.

### Processing

Built-in processing primitives cover:

- JSON
- XML
- CSV
- HTML
- feeds
- PDF inputs
- composable processing pipelines

Application-specific interpretation remains outside the framework core.

### Plugins

ECF provides a Python entry-point based plugin subsystem with separate:

- discovery
- loading
- lifecycle management
- registration

Plugin discovery is metadata-only and does **not** import third-party plugin
code.

This allows commands such as plugin listing and inspection to remain
side-effect-conscious discovery operations.

### Events and workers

The event subsystem provides:

- in-memory queues
- durable SQLite queues
- claim-token ownership
- leases
- expired-lease recovery
- ACK/NACK ownership validation
- workers
- retry policies
- exponential backoff
- durable scheduled retries
- configurable retry delay caps
- optional retry jitter
- in-memory dead-letter queues
- durable SQLite dead-letter queues
- persistence and reopen behavior
- concurrency-safe claim semantics

ECF does not claim exactly-once execution. Applications should remain
idempotency-friendly when handlers may be retried or redelivered.

## Public API

The top-level package intentionally exposes a small primary API:

```python
from enterprise_crawler import (
    BaseBot,
    Crawler,
    ExecutionResult,
    ExecutionStatus,
)
```

Version metadata is also available:

```python
from enterprise_crawler import (
    __version__,
    __title__,
    FRAMEWORK_NAME,
)
```

Subsystem-specific APIs remain in their dedicated namespaces, including:

```text
enterprise_crawler.config
enterprise_crawler.events
enterprise_crawler.plugins
enterprise_crawler.processing
enterprise_crawler.storage
```

Keeping the top-level API small limits unnecessary compatibility commitments.

## Command-line interface

Show the installed framework version:

```bash
enterprise-crawler --version
```

or:

```bash
enterprise-crawler version
```

Run local framework health checks:

```bash
enterprise-crawler doctor
```

Discover installed plugin entry points:

```bash
enterprise-crawler plugins list
```

Inspect plugin metadata:

```bash
enterprise-crawler plugins inspect <PLUGIN_NAME>
```

`plugins list` and `plugins inspect` perform discovery and metadata inspection;
they do not instantiate or execute plugin code.

## Failure model

ECF uses explicit framework contracts and subsystem-specific failure domains
instead of treating every failure as an interchangeable exception.

Important runtime distinctions include:

```text
completed
failed
cancelled
degraded
skipped
```

Cancellation is treated as a lifecycle outcome rather than being silently
collapsed into generic failure.

Critical persistence and ownership paths prefer fail-closed behavior where
silently continuing could lose data or violate ownership semantics.

## Design principles

The framework is developed around the following principles:

- explicit contracts
- small public surface
- low coupling
- dependency inversion
- dependency injection
- deterministic defaults
- fail-fast configuration
- fail-closed critical operations
- explicit resource ownership
- mutation isolation
- concurrency safety
- durable restart behavior
- backward compatibility
- Semantic Versioning
- no application-specific business rules in the framework core

## Development

Clone the repository:

```bash
git clone https://github.com/canerenaltungul/enterprise-crawler-framework.git
cd enterprise-crawler-framework
```

Install the package from the repository:

```bash
python -m pip install .
```

Run the test suite:

```bash
python -m pytest tests -q
```

Run the basic example:

```bash
python examples/basic_bot/hello_bot.py
```

Contributor guidance is available in
[CONTRIBUTING.md](https://github.com/canerenaltungul/enterprise-crawler-framework/blob/main/CONTRIBUTING.md).

## Project links

- PyPI: https://pypi.org/project/enterprise-crawler-framework/
- Source: https://github.com/canerenaltungul/enterprise-crawler-framework
- Issues: https://github.com/canerenaltungul/enterprise-crawler-framework/issues
- Releases: https://github.com/canerenaltungul/enterprise-crawler-framework/releases
- Changelog: https://github.com/canerenaltungul/enterprise-crawler-framework/blob/main/CHANGELOG.md
- Security policy: https://github.com/canerenaltungul/enterprise-crawler-framework/blob/main/SECURITY.md

## License

Enterprise Crawler Framework Community is released under the
[MIT License](https://github.com/canerenaltungul/enterprise-crawler-framework/blob/main/LICENSE).
