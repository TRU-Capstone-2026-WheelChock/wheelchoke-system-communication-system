import pytest
import time
import threading
import asyncio
import zmq 
from msg_handler import (
    get_publisher, get_subscriber, get_async_subscriber,
    ZmqPubOptions, ZmqSubOptions, SensorMessage
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
            "sensor_status_code": 200
        }
    )

def test_zmq_pub_sub_sync():
    """
    Test synchronous ZMQ message exchange using a background thread.
    """
    endpoint = "tcp://127.0.0.1:5556"
    received_msgs = []
    
    # --- 1. Receiver Setup (Subscriber) ---
    def run_subscriber():
        # Bind as server and wait for connection
        sub_opts = ZmqSubOptions(endpoint=endpoint, is_bind=True)
        
        with get_subscriber(sub_opts) as sub:
            # Prevent infinite hang: set 2000ms receive timeout
            sub.socket.setsockopt(zmq.RCVTIMEO, 2000)
            
            try:
                for msg in sub:
                    received_msgs.append(msg)
                    break 
            except zmq.Again:
                print("TIMEOUT: Subscriber did not receive message.")

    # Start subscriber thread
    sub_thread = threading.Thread(target=run_subscriber)
    sub_thread.start()
    
    # Allow time for socket to bind
    time.sleep(0.2)

    # --- 2. Sender Setup (Publisher) ---
    pub_opts = ZmqPubOptions(endpoint=endpoint) # Default is_connect=True
    
    with get_publisher(pub_opts) as pub:
        # Note: Sleep required to avoid 'Slow Joiner' problem in ZMQ
        time.sleep(0.5) 
        
        msg = create_valid_message("sync_tester")
        pub.send(msg)
    
    # --- 3. Cleanup & Verification ---
    sub_thread.join(timeout=3.0)
    
    assert len(received_msgs) == 1
    assert received_msgs[0].payload.sensor_status == "active"


@pytest.mark.asyncio
async def test_zmq_pub_sub_async():
    """
    Test asynchronous ZMQ message exchange using asyncio tasks.
    """
    endpoint = "tcp://127.0.0.1:5557"
    
    # --- 1. Async Receiver Task ---
    async def run_subscriber():
        opts = ZmqSubOptions(endpoint=endpoint, is_bind=True)
        async with get_async_subscriber(opts) as sub:
            async for msg in sub:
                return msg

    sub_task = asyncio.create_task(run_subscriber())
    
    # Wait for bind setup
    await asyncio.sleep(0.2)

    # --- 2. Async Sender ---
    pub_opts = ZmqPubOptions(endpoint=endpoint)
    with get_publisher(pub_opts) as pub:
        # Note: Small delay to ensure PUB/SUB handshake completes
        await asyncio.sleep(0.5)
        
        msg = create_valid_message("async_tester")
        pub.send(msg)

    # --- 3. Verification with Timeout ---
    try:
        received_msg = await asyncio.wait_for(sub_task, timeout=3.0)
        assert received_msg.payload.sensor_status == "active"
    except asyncio.TimeoutError:
        pytest.fail("Async Subscriber timed out! (Message not received)")