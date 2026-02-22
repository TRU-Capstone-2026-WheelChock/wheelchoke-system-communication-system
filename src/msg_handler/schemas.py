# src/msg_handler/schemas.py
from datetime import datetime
from typing import Literal, TypeAlias

from pydantic import BaseModel, Field, TypeAdapter, field_serializer

"""
Payload definitions.
Note: Pydantic parses Union types by field matching (first match wins). 
BE CAREFUL WITH FIELD OVERLAPS! Consider using 'Discriminator' if payload types increase.
"""


class SensorPayload(BaseModel):
    """
    Payload for human detection sensor data.

    Attributes:
        isThereHuman: True if a human is detected.
        human_exist_possibility: possibility of human existence; not available then None. or max 100.0
        sensor_status: Operational status string.
        sensor_status_code: Numeric status code.
    """

    isThereHuman: bool
    human_exist_possibility: float | None = Field(default=None, ge=0.0, le=100.0)
    sensor_status: str
    sensor_status_code: int
    # uptime : timedelta


class HeartBeatPayload(BaseModel):
    """
    Payload for device health status (heartbeat).

    Attributes:
        status: Device health status (e.g., 'Active').
        status_code: Numeric health code.

    Note:
        expected status information to send, is following
        - Display: status of the display, starting up, Running, Error
        - Motor: status of the motor, starting up, folded
    """

    status: str
    status_code: int
    # uptime : timedelta


class SensorMessage(BaseModel):
    """
    Generic envelope for sensor telemetry.

    Attributes:
        sender_id: Unique device identifier.
        sender_name: Optional human-readable device name.
        timestamp: Event creation time.
        data_type: Discriminator for the payload type.
        payload: The actual data content (SensorPayload or HeartBeatPayload).

    Note:
        - When serialized to JSON, `timestamp` is formatted as 'YYYY-MM-DD HH:MM'.
        - [Important] The `payload` is validated against classes in T (first match wins).
          Ensure that the dictionary keys do not overlap significantly
          between these types to avoid incorrect type inference.
    """

    sender_id: str
    sender_name: str | None
    timestamp: datetime = Field(default_factory=datetime.now)
    data_type: str
    payload: SensorPayload | HeartBeatPayload
    sequence_no: int = 0

    @field_serializer("timestamp")
    def serialize_timestamp(self, dt: datetime, _info):
        return dt.strftime("%Y-%m-%d %H:%M")
    
    def get_status(self):
        """get status

        Returns:
            tuple[int, str]: status_code, status(str)
        """
        if isinstance(self.payload, SensorPayload):
            return self.payload.sensor_status_code, self.payload.sensor_status
        if isinstance(self.payload, HeartBeatPayload):
            return self.payload.status_code, self.payload.status
        
        raise NotImplementedError(f"{type(self.payload)} does not expected to have status")


class SensorDisplayMode(BaseModel):
    sensor_name: str
    is_there_human : bool
    human_exist_possibility: float | None = Field(default=None, ge=0.0, le=100.0)


class DisplayMessage(BaseModel):
    sender_id: str
    timestamp : datetime = Field(default_factory=datetime.now)
    is_override_mode : bool
    sensor_display_dict : dict[str, SensorDisplayMode] = Field(default_factory=dict)
    moter_mode : str

class MotorMessage(BaseModel):
    sender_id : str
    timestamp : datetime = Field(default_factory=datetime.now)
    is_override_mode : bool
    ordered_mode : str


SupportedMessage: TypeAlias = SensorMessage | DisplayMessage | MotorMessage
ExpectedMessageType: TypeAlias = Literal["auto", "sensor", "display", "motor"]

_supported_message_adapter = TypeAdapter(SupportedMessage)
_sensor_message_adapter = TypeAdapter(SensorMessage)
_display_message_adapter = TypeAdapter(DisplayMessage)
_motor_message_adapter = TypeAdapter(MotorMessage)


def parse_message_json(
    json_str: str, expected_type: ExpectedMessageType = "auto"
) -> SupportedMessage:
    """Parse JSON string into one supported type, optionally constrained by expected_type."""
    if expected_type == "auto":
        return _supported_message_adapter.validate_json(json_str)
    if expected_type == "sensor":
        return _sensor_message_adapter.validate_json(json_str)
    if expected_type == "display":
        return _display_message_adapter.validate_json(json_str)
    if expected_type == "motor":
        return _motor_message_adapter.validate_json(json_str)

    raise ValueError(f"Unexpected expected_type: {expected_type}")
