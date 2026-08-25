from .base import EnterpriseCrawlerError


class NetworkError(EnterpriseCrawlerError):
    """Genel ağ hatası."""
    default_message = "Network request failed."


class RetryableNetworkError(NetworkError):
    """Tekrar denenebilir ağ hatası."""
    default_message = "Temporary network error."


class CircuitBreakerOpenError(NetworkError):
    """Circuit breaker açık."""
    default_message = "Circuit breaker is open."


class DownloadError(NetworkError):
    """Dosya indirme işlemi güvenli biçimde tamamlanamadı."""
    default_message = "Download failed."