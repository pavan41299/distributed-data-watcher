"""Validate the input format described in the assessment."""
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, TypeAdapter


class SensorReading(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int = Field(strict=True, ge=0)
    value: float = Field(allow_inf_nan=False)
    timestamp: AwareDatetime


_READINGS = TypeAdapter(list[SensorReading])


def parse_readings(raw_json: bytes) -> list[SensorReading]:
    """Parse JSON array; reject invalid, empty, or malformed telemetry."""
    readings = _READINGS.validate_json(raw_json)
    if not readings:
        raise ValueError("The readings array must not be empty")
    return readings
