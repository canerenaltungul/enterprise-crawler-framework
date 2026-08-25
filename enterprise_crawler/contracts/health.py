from dataclasses import dataclass

from .enums import HealthState


@dataclass(slots=True)
class HealthStatus:

    state: HealthState

    message: str = ""