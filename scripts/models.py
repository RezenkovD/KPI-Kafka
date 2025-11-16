from faust import Record
from typing import Optional


class TurbineTelemetry(Record):
    """Модель телеметрії вітрової турбіни"""
    device_id: str
    timestamp: str
    power_output: float
    wind_speed: float
    wind_direction: float
    blade_pitch: float
    vibration: float
    temperature_generator: float
    temperature_gearbox: float


class CurtailmentRequest(Record):
    """Модель запиту на обмеження потужності (Saga)"""
    device_id: str
    reason: str


class CancelCurtailment(Record):
    """Модель запиту на скасування обмеження (Compensation)"""
    device_id: str
    reason: Optional[str] = None

