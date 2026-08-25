from .base import EnterpriseCrawlerError


class PluginError(EnterpriseCrawlerError):
    default_message = "Plugin execution failed."


class PluginRegistrationError(PluginError):
    default_message = "Plugin registration failed."


class PluginValidationError(PluginError):
    default_message = "Plugin validation failed."