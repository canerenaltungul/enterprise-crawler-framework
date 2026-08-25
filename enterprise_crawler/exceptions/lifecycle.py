from .base import EnterpriseCrawlerError


class LifecycleError(EnterpriseCrawlerError):
    default_message = "Lifecycle operation failed."


class ShutdownRequested(LifecycleError):
    default_message = "Shutdown requested."


class AlreadyRunningError(LifecycleError):
    default_message = "Crawler is already running."