import pytest
import time
import threading
import asyncio
import zmq
from msg_handler import (
    get_publisher,
    get_subscriber,
    get_async_subscriber,
    ZmqPubOptions,
    ZmqSubOptions,
    SensorMessage,
)


def create_valid_message(sender_id: str) -> SensorMessage:
    """
    Helper to create a standard SensorMessage for testing.

    Attributes:
        sender_id: Unique identifier for the sending entity.
    """
    return SensorMessage(
        sender_id=sender_id,
        sender_name="Tester",
        data_type="test",
        payload={
            "isThereHuman": False,
            "sensor_status": "active",
            "sensor_status_code": 200,
        },
    )


def test_zmq_pub_sub_sync():
    """
    Integration test for synchronous ZMQ PUB/SUB using threading.
    """
    endpoint = "tcp://127.0.0.1:5556"
    received_msgs = []

    # --- 1. Subscriber Setup ---
    def run_subscriber():
        # Set is_bind=True to act as the server/host
        sub_opts = ZmqSubOptions(endpoint=endpoint, is_bind=True)

        with get_subscriber(sub_opts) as sub:
            # Set 2000ms receive timeout to prevent test hanging
            sub.socket.setsockopt(zmq.RCVTIMEO, 2000)

            try:
                for msg in sub:
                    received_msgs.append(msg)
                    break
            except zmq.Again:
                print("TIMEOUT: Subscriber did not receive message.")

    # Start receiver thread
    sub_thread = threading.Thread(target=run_subscriber)
    sub_thread.start()

    # Wait for the Subscriber to finish binding
    time.sleep(0.2)

    # --- 2. Publisher Setup ---
    pub_opts = ZmqPubOptions(endpoint=endpoint)  # is_connect=True by default

    with get_publisher(pub_opts) as pub:
        # Note: Sleep is critical here to mitigate the ZMQ 'Slow Joiner' phenomenon.
        # It ensures the TCP handshake and subscription filter are processed.
        time.sleep(0.5)

        msg = create_valid_message("sync_tester")
        pub.send(msg)

    # --- 3. Verification ---
    sub_thread.join(timeout=3.0)

    assert len(received_msgs) == 1
    assert received_msgs[0].payload.sensor_status == "active"


@pytest.mark.asyncio
async def test_zmq_pub_sub_async():
    """
    Integration test for asynchronous ZMQ PUB/SUB using asyncio tasks.
    """
    endpoint = "tcp://127.0.0.1:5557"

    # --- 1. Async Subscriber Task ---
    async def run_subscriber():
        opts = ZmqSubOptions(endpoint=endpoint, is_bind=True)
        async with get_async_subscriber(opts) as sub:
            async for msg in sub:
                return msg

    sub_task = asyncio.create_task(run_subscriber())

    # Wait for Bind to initialize
    await asyncio.sleep(0.2)

    # --- 2. Publisher Setup ---
    pub_opts = ZmqPubOptions(endpoint=endpoint)
    with get_publisher(pub_opts) as pub:
        # Note: Mandatory delay for the async Slow Joiner fix.
        # Without this, the message is sent before the peer is fully ready.
        await asyncio.sleep(0.5)

        msg = create_valid_message("async_tester")
        pub.send(msg)

    # --- 3. Verification with timeout ---
    try:
        received_msg = await asyncio.wait_for(sub_task, timeout=3.0)
        assert received_msg.payload.sensor_status == "active"
    except asyncio.TimeoutError:
        pytest.fail("Async Subscriber timed out! (Message not received)")
