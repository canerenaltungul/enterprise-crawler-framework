from .base import EnterpriseCrawlerError


class StorageError(EnterpriseCrawlerError):
    default_message = "Storage operation failed."


class AtomicWriteError(StorageError):
    default_message = "Atomic write failed."