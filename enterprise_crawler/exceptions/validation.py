from .base import EnterpriseCrawlerError


class ValidationError(EnterpriseCrawlerError):
    default_message = "Validation failed."


class ContractValidationError(ValidationError):
    default_message = "Contract validation failed."