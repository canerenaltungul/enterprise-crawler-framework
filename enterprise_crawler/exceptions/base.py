from __future__ import annotations


class EnterpriseCrawlerError(Exception):
    """
    Enterprise Crawler Framework içindeki bütün özel hataların temel sınıfı.
    """

    default_message = "Enterprise Crawler error."

    def __init__(self, message: str | None = None):
        super().__init__(message or self.default_message)