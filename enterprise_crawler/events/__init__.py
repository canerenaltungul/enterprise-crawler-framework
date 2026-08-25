from __future__ import annotations

"""
Enterprise Crawler Framework - Events Public API

Event runtime subsystem için kararlı public import yüzeyi.

Desteklenen queue backend'leri:

- InMemoryEventQueue
- SQLiteEventQueue

Dead-letter backend'leri:

- InMemoryDeadLetterQueue
- SQLiteDeadLetterQueue
"""

# =============================================================================
# QUEUE CONTRACT / MEMORY
# =============================================================================
from enterprise_crawler.events.queue import (
    ClaimedEvent,
    DuplicateEventMessageError,
    EventClaimOwnershipError,
    EventNotClaimedError,
    EventQueueClosedError,
    EventQueueError,
    EventQueueProtocol,
    EventQueueValidationError,
    InMemoryEventQueue,
    PublishedEvent,
    UnknownEventMessageError,
)


# =============================================================================
# SQLITE EVENT QUEUE
# =============================================================================
from enterprise_crawler.events.sqlite_queue import (
    SQLiteEventQueue,
    SQLiteEventQueueError,
)


# =============================================================================
# DISPATCHER
# =============================================================================
from enterprise_crawler.events.dispatcher import (
    DispatchResult,
    DuplicateEventHandlerError,
    EventDispatcher,
    EventDispatcherError,
    EventDispatcherValidationError,
    EventHandlerExecutionError,
    EventHandlerNotFoundError,
)


# =============================================================================
# WORKER
# =============================================================================
from enterprise_crawler.events.worker import (
    EventWorker,
    EventWorkerAlreadyRunningError,
    EventWorkerClosedError,
    EventWorkerDeadLetterError,
    EventWorkerError,
    EventWorkerValidationError,
    WorkerRunSummary,
)


# =============================================================================
# RETRY
# =============================================================================
from enterprise_crawler.events.retry import (
    RetryAction,
    RetryDecision,
    RetryPolicy,
    RetryPolicyError,
    RetryPolicyValidationError,
)


# =============================================================================
# DEAD LETTER CONTRACT / MEMORY
# =============================================================================
from enterprise_crawler.events.dead_letter import (
    DeadLetterQueue,
    DeadLetterQueueError,
    DeadLetterQueueProtocol,
    DeadLetterRecord,
    DeadLetterValidationError,
    DuplicateDeadLetterError,
    InMemoryDeadLetterQueue,
    UnknownDeadLetterError,
)


# =============================================================================
# SQLITE DEAD LETTER
# =============================================================================
from enterprise_crawler.events.sqlite_dead_letter import (
    SQLiteDeadLetterQueue,
    SQLiteDeadLetterQueueClosedError,
    SQLiteDeadLetterQueueError,
)


# =============================================================================
# PUBLIC API
# =============================================================================
__all__ = [
    # Queue contract
    "EventQueueProtocol",

    # Common queue contracts
    "PublishedEvent",
    "ClaimedEvent",

    # In-memory queue
    "InMemoryEventQueue",

    # SQLite event queue
    "SQLiteEventQueue",
    "SQLiteEventQueueError",

    # Queue errors
    "EventQueueError",
    "EventQueueValidationError",
    "EventQueueClosedError",
    "DuplicateEventMessageError",
    "UnknownEventMessageError",
    "EventNotClaimedError",
    "EventClaimOwnershipError",

    # Dispatcher
    "EventDispatcher",
    "DispatchResult",
    "EventDispatcherError",
    "EventDispatcherValidationError",
    "DuplicateEventHandlerError",
    "EventHandlerNotFoundError",
    "EventHandlerExecutionError",

    # Worker
    "EventWorker",
    "WorkerRunSummary",
    "EventWorkerError",
    "EventWorkerValidationError",
    "EventWorkerAlreadyRunningError",
    "EventWorkerClosedError",
    "EventWorkerDeadLetterError",

    # Retry
    "RetryAction",
    "RetryDecision",
    "RetryPolicy",
    "RetryPolicyError",
    "RetryPolicyValidationError",

    # Dead Letter contract / memory
    "DeadLetterQueueProtocol",
    "DeadLetterQueue",
    "InMemoryDeadLetterQueue",
    "DeadLetterRecord",
    "DeadLetterQueueError",
    "DeadLetterValidationError",
    "DuplicateDeadLetterError",
    "UnknownDeadLetterError",

    # SQLite Dead Letter
    "SQLiteDeadLetterQueue",
    "SQLiteDeadLetterQueueError",
    "SQLiteDeadLetterQueueClosedError",
]