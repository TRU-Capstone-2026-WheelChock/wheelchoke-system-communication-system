import pytest
import json
from datetime import datetime
from pydantic import ValidationError
from msg_handler.schemas import SensorMessage, SensorPayload, HeartBeatPayload

# --- Normal Case Tests ---


def test_create_valid_sensor_message():
    """Verify if data matching SensorPayload structure is correctly converted."""
    data = {
        "sender_id": "sensor_unit_01",
        "sender_name": "Entrance Camera",
        "data_type": "human_detection",
        "payload": {
            "isThereHuman": True,
            "sensor_status": "detecting",
            "sensor_status_code": 101,
        },
    }
    msg = SensorMessage(**data)

    # Verification
    assert msg.sender_id == "sensor_unit_01"
    # Should automatically become SensorPayload type based on fields
    assert isinstance(msg.payload, SensorPayload)
    assert msg.payload.isThereHuman is True
    assert msg.payload.sensor_status_code == 101
    assert msg.payload.human_exist_possibility == None


def test_create_valid_sensor_message_human_possibility():
    """Verify if data matching SensorPayload structure is correctly converted."""
    data = {
        "sender_id": "sensor_unit_01",
        "sender_name": "Entrance Camera",
        "data_type": "human_detection",
        "payload": {
            "isThereHuman": True,
            "sensor_status": "detecting",
            "sensor_status_code": 101,
            "human_exist_possibility" : 25.25

        },
    }
    msg = SensorMessage(**data)

    # Verification
    assert msg.sender_id == "sensor_unit_01"
    # Should automatically become SensorPayload type based on fields
    assert isinstance(msg.payload, SensorPayload)
    assert msg.payload.isThereHuman is True
    assert msg.payload.sensor_status_code == 101
    assert msg.payload.human_exist_possibility == 25.25

def test_create_valid_heartbeat_message():
    """Verify if data matching HeartBeatPayload structure is correctly converted (Union discrimination)."""
    data = {
        "sender_id": "system_mon",
        "sender_name": None,  # Check optional field
        "data_type": "health_check",
        "payload": {"status": "Alive", "status_code": 200},
    }
    msg = SensorMessage(**data)

    # Verification
    assert isinstance(msg.payload, HeartBeatPayload)
    assert msg.payload.status == "Alive"
    assert msg.sender_name is None  # Check if None is allowed


def test_timestamp_auto_generation():
    """Verify if timestamp is automatically generated when not provided."""
    msg = SensorMessage(
        sender_id="test",
        sender_name="test",
        data_type="test",
        payload={"status": "ok", "status_code": 200},
    )
    assert isinstance(msg.timestamp, datetime)
    # Check if generated time is close to 'now' (rough check < 1.0s)
    assert (datetime.now() - msg.timestamp).total_seconds() < 1.0


def test_json_serialization_format():
    """Verify if timestamp format is converted during JSON serialization."""
    dt = datetime(2025, 12, 31, 23, 59, 0)
    msg = SensorMessage(
        sender_id="test",
        sender_name="test",
        timestamp=dt,
        data_type="test",
        payload={"status": "ok", "status_code": 200},
    )

    json_str = msg.model_dump_json()
    json_dict = json.loads(json_str)

    # Check custom serializer format ("%Y-%m-%d %H:%M")
    assert json_dict["timestamp"] == "2025-12-31 23:59"
    # Seconds should be truncated
    assert len(json_dict["timestamp"].split(":")) == 2


# --- Error Case Tests ---


def test_validation_error_missing_payload_field():
    """Verify if error occurs when required fields are missing."""
    with pytest.raises(ValidationError) as excinfo:
        SensorMessage(
            sender_id="fail",
            sender_name="fail",
            data_type="fail",
            payload={
                "status": "Alive"
                # status_code is missing -> Invalid as HeartBeatPayload
            },
        )
    # Check if error message contains payload-related hints
    assert "payload" in str(excinfo.value)


def test_validation_error_invalid_type():
    """Verify if error occurs when field types mismatch."""
    with pytest.raises(ValidationError):
        SensorMessage(
            sender_id="fail",
            sender_name="fail",
            data_type="fail",
            payload={
                "isThereHuman": "NotBoolean",  # String provided where bool expected
                "sensor_status": "ok",
                "sensor_status_code": 1,
            },
        )


def test_validation_error_unknown_payload_structure():
    """Verify if error occurs when undefined payload structure is provided."""
    with pytest.raises(ValidationError):
        SensorMessage(
            sender_id="fail",
            sender_name="fail",
            data_type="fail",
            payload={
                "unknown_key": 12345
                # Fits neither SensorPayload nor HeartBeatPayload
            },
        )
