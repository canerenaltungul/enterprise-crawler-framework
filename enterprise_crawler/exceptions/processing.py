from .base import EnterpriseCrawlerError


class ProcessingError(EnterpriseCrawlerError):
    default_message = "Processing failed."