from .base import EnterpriseCrawlerError


class ConfigurationError(EnterpriseCrawlerError):
    """Framework konfigürasyonu geçersiz."""
    default_message = "Invalid framework configuration."


class MissingConfigurationError(ConfigurationError):
    """Zorunlu konfigürasyon bulunamadı."""
    default_message = "Required configuration is missing."