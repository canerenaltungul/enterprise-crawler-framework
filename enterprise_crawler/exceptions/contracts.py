from .base import EnterpriseCrawlerError


class ContractError(EnterpriseCrawlerError):
    default_message = "Contract violation detected."