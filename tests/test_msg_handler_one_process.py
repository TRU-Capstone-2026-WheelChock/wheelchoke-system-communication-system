import pytest
import time
import threading
import asyncio
import contextlib
import zmq
import zmq.asyncio
from msg_handler import (
    get_publisher,
    get_async_publisher,
    get_subscriber,
    get_async_subscriber,
    ZmqPubOptions,
    ZmqSubOptions,
    SensorMessage,
    SensorPayload,
    GenericMessageDatatype,
    DisplayMessage,
    SensorDisplayMode,
    MotorState,
    MotorMessage,
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
        data_type=GenericMessageDatatype.SENSOR,
        payload=SensorPayload(
            isThereHuman=False,
            sensor_status="active",
            sensor_status_code=200,
        ),
    )


def create_display_message(sender_id: str) -> DisplayMessage:
    return DisplayMessage(
        sender_id=sender_id,
        is_override_mode=False,
        sensor_display_dict={
            "front": SensorDisplayMode(
                sensor_name="front_sensor",
                is_there_human=True,
                human_exist_possibility=88.0,
            )
        },
        moter_mode=MotorState.DEPLOYING,
    )


def create_motor_message(sender_id: str) -> MotorMessage:
    return MotorMessage(
        sender_id=sender_id,
        is_override_mode=False,
        ordered_mode=MotorState.DEPLOYING,
    )


def test_zmq_pub_sub_sync():
    """
    Integration test for synchronous ZMQ PUB/SUB using threading.
    """
    endpoint = "tcp://127.0.0.1:5556"
    topic = "sensor"
    received_msgs = []
    shared_ctx = zmq.Context()

    try:
        # --- 1. Subscriber Setup ---
        def run_subscriber():
            # Set is_bind=True to act as the server/host
            sub_opts = ZmqSubOptions(
                endpoint=endpoint,
                is_bind=True,
                topics=[f"{topic} "],
                context=shared_ctx,
            )

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
        pub_opts = ZmqPubOptions(
            endpoint=endpoint,
            topic=topic,
            context=shared_ctx,
        )  # is_connect=True by default

        with get_publisher(pub_opts) as pub:
            # Note: Sleep is critical here to mitigate the ZMQ 'Slow Joiner' phenomenon.
            # It ensures the TCP handshake and subscription filter are processed.
            time.sleep(0.5)

            msg = create_valid_message("sync_tester")
            pub.send(msg)

        # --- 3. Verification ---
        sub_thread.join(timeout=3.0)

        assert len(received_msgs) == 1
        assert isinstance(
            received_msgs[0], SensorMessage
        ), "it is not SensorMessage instance"
        assert received_msgs[0].payload.sensor_status == "active"
    finally:
        shared_ctx.term()


@pytest.mark.asyncio
async def test_zmq_pub_sub_async():
    """
    Integration test for asynchronous ZMQ PUB/SUB using asyncio tasks.
    """
    endpoint = "tcp://127.0.0.1:5557"
    topic = "sensor"
    shared_ctx = zmq.asyncio.Context()

    # --- 1. Async Subscriber Task ---
    async def run_subscriber():
        opts = ZmqSubOptions(
            endpoint=endpoint,
            is_bind=True,
            topics=[f"{topic} "],
            context=shared_ctx,
        )
        async with get_async_subscriber(opts) as sub:
            async for msg in sub:
                return msg

    sub_task = asyncio.create_task(run_subscriber())

    try:
        # Wait for Bind to initialize
        await asyncio.sleep(0.2)

        # --- 2. Publisher Setup ---
        pub_opts = ZmqPubOptions(endpoint=endpoint, topic=topic, context=shared_ctx)
        async with get_async_publisher(pub_opts) as pub:
            # Note: Mandatory delay for the async Slow Joiner fix.
            # Without this, the message is sent before the peer is fully ready.
            await asyncio.sleep(0.5)

            msg = create_valid_message("async_tester")
            await pub.send(msg)

        # --- 3. Verification with timeout ---
        received_msg = await asyncio.wait_for(sub_task, timeout=3.0)
        assert received_msg.payload.sensor_status == "active"
    except asyncio.TimeoutError:
        pytest.fail("Async Subscriber timed out! (Message not received)")
    finally:
        if not sub_task.done():
            sub_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sub_task
        shared_ctx.term()


def test_zmq_pub_sub_sync_expected_display():
    endpoint = "tcp://127.0.0.1:5558"
    received_msgs = []
    shared_ctx = zmq.Context()

    try:
        def run_subscriber():
            sub_opts = ZmqSubOptions(
                endpoint=endpoint,
                is_bind=True,
                expected_type="display",
                context=shared_ctx,
            )
            with get_subscriber(sub_opts) as sub:
                sub.socket.setsockopt(zmq.RCVTIMEO, 2000)
                try:
                    for msg in sub:
                        received_msgs.append(msg)
                        break
                except zmq.Again:
                    print("TIMEOUT: Subscriber did not receive message.")

        sub_thread = threading.Thread(target=run_subscriber)
        sub_thread.start()
        time.sleep(0.2)

        pub_opts = ZmqPubOptions(endpoint=endpoint, context=shared_ctx)
        with get_publisher(pub_opts) as pub:
            time.sleep(0.5)
            pub.send(create_display_message("display_sync_tester"))

        sub_thread.join(timeout=3.0)

        assert len(received_msgs) == 1
        assert isinstance(received_msgs[0], DisplayMessage)
        assert received_msgs[0].moter_mode == MotorState.DEPLOYING
    finally:
        shared_ctx.term()


@pytest.mark.asyncio
async def test_zmq_pub_sub_async_expected_motor():
    endpoint = "tcp://127.0.0.1:5559"
    shared_ctx = zmq.asyncio.Context()

    async def run_subscriber():
        opts = ZmqSubOptions(
            endpoint=endpoint,
            is_bind=True,
            expected_type="motor",
            context=shared_ctx,
        )
        async with get_async_subscriber(opts) as sub:
            async for msg in sub:
                return msg

    sub_task = asyncio.create_task(run_subscriber())
    try:
        await asyncio.sleep(0.2)

        pub_opts = ZmqPubOptions(endpoint=endpoint, context=shared_ctx)
        async with get_async_publisher(pub_opts) as pub:
            await asyncio.sleep(0.5)
            await pub.send(create_motor_message("motor_async_tester"))

        received_msg = await asyncio.wait_for(sub_task, timeout=3.0)
        assert isinstance(received_msg, MotorMessage)
        assert received_msg.ordered_mode == MotorState.DEPLOYING
    except asyncio.TimeoutError:
        pytest.fail("Async Subscriber timed out! (MotorMessage not received)")
    finally:
        if not sub_task.done():
            sub_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sub_task
        shared_ctx.term()


@pytest.mark.asyncio
async def test_zmq_pub_sub_async_single_topic_filter() ->None:
    endpoint = "tcp://127.0.0.1:5560"
    shared_ctx = zmq.asyncio.Context()

    async def run_subscriber():
        opts = ZmqSubOptions(
            endpoint=endpoint,
            is_bind=True,
            # Include trailing space to match the actual "topic + space + json" prefix.
            topics=["sensor "],
            expected_type="sensor",
            context=shared_ctx,
        )
        received: list[SensorMessage] = []
        async with get_async_subscriber(opts) as sub:
            async for msg in sub:
                received.append(msg)
                return received

    sub_task = asyncio.create_task(run_subscriber())

    try:
        await asyncio.sleep(0.2)

        pub_a_opts = ZmqPubOptions(endpoint=endpoint, topic="sensor", context=shared_ctx)
        pub_b_opts = ZmqPubOptions(
            endpoint=endpoint, topic="displayHB", context=shared_ctx
        )
        pub_c_opts = ZmqPubOptions(endpoint=endpoint, topic="", context=shared_ctx)

        async with get_async_publisher(pub_a_opts) as pub_a, get_async_publisher(
            pub_b_opts
        ) as pub_b, get_async_publisher(pub_c_opts) as pub_c:
            # Allow PUB sockets to finish handshake before sending.
            await asyncio.sleep(0.5)
            # Send non-matching topics first; subscriber should ignore them.
            await pub_b.send(create_valid_message("pub_b_displayHB_topic"))
            await pub_c.send(create_valid_message("pub_c_no_topic"))
            await pub_a.send(create_valid_message("pub_a_sensor_topic"))

        received_msgs = await asyncio.wait_for(sub_task, timeout=3.0)
        assert len(received_msgs) == 1
        assert isinstance(received_msgs[0], SensorMessage)
        assert received_msgs[0].sender_id == "pub_a_sensor_topic"
    except asyncio.TimeoutError:
        pytest.fail("Async Subscriber timed out! (Filtered topic message not received)")
    finally:
        if not sub_task.done():
            sub_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sub_task
        shared_ctx.term()


@pytest.mark.asyncio
async def test_zmq_pub_sub_async_subscribe_all_mixed_topics()->None:
    endpoint = "tcp://127.0.0.1:5561"
    shared_ctx = zmq.asyncio.Context()

    async def run_subscriber():
        opts = ZmqSubOptions(
            endpoint=endpoint,
            is_bind=True,
            topics=[""],
            expected_type="sensor",
            context=shared_ctx,
        )
        received: list[SensorMessage] = []
        async with get_async_subscriber(opts) as sub:
            async for msg in sub:
                received.append(msg)
                if len(received) == 3:
                    return received

    sub_task = asyncio.create_task(run_subscriber())

    try:
        await asyncio.sleep(0.2)

        pub_a_opts = ZmqPubOptions(endpoint=endpoint, topic="sensor", context=shared_ctx)
        pub_b_opts = ZmqPubOptions(
            endpoint=endpoint, topic="displayHB", context=shared_ctx
        )
        pub_c_opts = ZmqPubOptions(endpoint=endpoint, topic="", context=shared_ctx)

        async with get_async_publisher(pub_a_opts) as pub_a, get_async_publisher(
            pub_b_opts
        ) as pub_b, get_async_publisher(pub_c_opts) as pub_c:
            await asyncio.sleep(0.5)
            await pub_a.send(create_valid_message("pub_a_sensor_topic"))
            await pub_b.send(create_valid_message("pub_b_displayHB_topic"))
            await pub_c.send(create_valid_message("pub_c_no_topic"))

        received_msgs = await asyncio.wait_for(sub_task, timeout=3.0)
        sender_ids = {msg.sender_id for msg in received_msgs}

        assert len(received_msgs) == 3
        assert sender_ids == {
            "pub_a_sensor_topic",
            "pub_b_displayHB_topic",
            "pub_c_no_topic",
        }
    except asyncio.TimeoutError:
        pytest.fail("Async Subscriber timed out! (Subscribe-all mixed topic messages not received)")
    finally:
        if not sub_task.done():
            sub_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sub_task
        shared_ctx.term()
