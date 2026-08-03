"""Unit tests for the optional loop-native AsyncioMqttDriver.

paho is mocked (like the rest of the suite). The event loop is usually a
MagicMock so we can assert add_reader/add_writer/remove_* and task scheduling
without a real selector; the async methods (start/stop/loops) are driven with
asyncio.run (no pytest-asyncio, no new dev dependency). The mock loop's
run_in_executor runs the target inline and returns a resolved Future so it is
awaitable more than once.

A few tests deliberately use a record-only call_soon_threadsafe so the
thread-bounce invariant is observable (a hook that mutated the loop directly
instead of scheduling would be caught).
"""

import asyncio
import subprocess
import sys
from unittest.mock import MagicMock

import paho.mqtt.client as mqtt
import pytest

from ebus_mqtt_client.asyncio_driver import AsyncioMqttDriver


def _coro_name(coro):
    return getattr(coro, "__qualname__", None) or coro.cr_code.co_name


def make_client():
    client = MagicMock()
    client.is_running = False
    client.mqttc = MagicMock()
    return client


def make_mock_loop():
    """A MagicMock loop with the few coroutine-aware behaviours the driver needs."""
    loop = MagicMock()
    # Run a bounced callback inline, as if the loop had scheduled it.
    loop.call_soon_threadsafe = lambda fn, *a: fn(*a)
    # Record scheduled coroutines by name, then close them (avoid "never awaited").
    scheduled = []

    def _capture(coro):
        scheduled.append(_coro_name(coro))
        coro.close()
        return MagicMock()

    loop.create_task = MagicMock(side_effect=_capture)
    loop.scheduled = scheduled  # test-visible list of scheduled coroutine names

    # run_in_executor: run the target inline, return a resolved (reusable) Future.
    def _run_in_executor(executor, fn, *args):
        fut = asyncio.get_running_loop().create_future()
        try:
            fut.set_result(fn(*args))
        except Exception as e:  # noqa: BLE001 - mirror executor result/exception into the future
            fut.set_exception(e)
        return fut

    loop.run_in_executor = _run_in_executor
    return loop


# -- start() --------------------------------------------------------------------


def test_hooks_not_wired_before_start():
    client = make_client()
    driver = AsyncioMqttDriver(client, loop=make_mock_loop())
    assert client.mqttc.on_socket_open is not driver._on_socket_open


def test_start_wires_hooks_flips_running_and_connects():
    client = make_client()
    loop = make_mock_loop()
    driver = AsyncioMqttDriver(client, loop=loop)

    asyncio.run(driver.start())

    assert client.mqttc.on_socket_open == driver._on_socket_open
    assert client.mqttc.on_socket_close == driver._on_socket_close
    assert client.mqttc.on_socket_register_write == driver._on_socket_register_write
    assert client.mqttc.on_socket_unregister_write == driver._on_socket_unregister_write
    assert client.is_running is True
    client.mqttc.reconnect.assert_called_once()  # blocking connect, via executor
    # Happy path must NOT arm a background reconnect loop.
    assert not any("_reconnect_loop" in n for n in loop.scheduled)


def test_start_schedules_backoff_reconnect_when_initial_connect_refused():
    client = make_client()
    client.mqttc.reconnect.side_effect = ConnectionRefusedError("broker down")
    loop = make_mock_loop()
    driver = AsyncioMqttDriver(client, loop=loop)

    asyncio.run(driver.start())  # must not raise

    assert client.is_running is True
    assert any("_reconnect_loop" in n for n in loop.scheduled)


def test_start_handles_valueerror_from_reconnect():
    # Bad host/port makes paho.reconnect() raise ValueError (not OSError); start()
    # must still hand it to the backoff loop rather than propagate.
    client = make_client()
    client.mqttc.reconnect.side_effect = ValueError("Invalid host.")
    loop = make_mock_loop()
    driver = AsyncioMqttDriver(client, loop=loop)

    asyncio.run(driver.start())  # must not raise

    assert any("_reconnect_loop" in n for n in loop.scheduled)


def test_second_start_raises():
    client = make_client()
    driver = AsyncioMqttDriver(client, loop=make_mock_loop())
    asyncio.run(driver.start())
    with pytest.raises(RuntimeError):
        asyncio.run(driver.start())


# -- socket hooks: thread-bounce invariant --------------------------------------


def test_socket_hooks_bounce_onto_loop_and_never_mutate_it_directly():
    # Record-only call_soon_threadsafe: if any hook mutated the loop directly
    # (instead of scheduling), the add_/remove_ assertions below would fail.
    client = make_client()
    loop = MagicMock()
    loop.call_soon_threadsafe = MagicMock()
    driver = AsyncioMqttDriver(client, loop=loop)
    sock = MagicMock()

    driver._on_socket_open(client.mqttc, None, sock)
    driver._on_socket_close(client.mqttc, None, sock)
    driver._on_socket_register_write(client.mqttc, None, sock)
    driver._on_socket_unregister_write(client.mqttc, None, sock)

    assert loop.call_soon_threadsafe.call_count == 4
    loop.add_reader.assert_not_called()
    loop.add_writer.assert_not_called()
    loop.remove_reader.assert_not_called()
    loop.remove_writer.assert_not_called()


def test_socket_open_registers_reader_and_schedules_misc():
    client = make_client()
    loop = make_mock_loop()
    driver = AsyncioMqttDriver(client, loop=loop)
    sock = MagicMock()

    driver._on_socket_open(client.mqttc, None, sock)

    assert driver._sock is sock
    loop.add_reader.assert_called_once_with(sock, client.mqttc.loop_read)
    assert any("_misc_loop" in n for n in loop.scheduled)


def test_socket_register_write_adds_writer():
    client = make_client()
    loop = make_mock_loop()
    driver = AsyncioMqttDriver(client, loop=loop)
    sock = MagicMock()

    driver._on_socket_register_write(client.mqttc, None, sock)

    loop.add_writer.assert_called_once_with(sock, client.mqttc.loop_write)


def test_socket_unregister_write_removes_writer():
    client = make_client()
    loop = make_mock_loop()
    driver = AsyncioMqttDriver(client, loop=loop)
    sock = MagicMock()

    driver._on_socket_unregister_write(client.mqttc, None, sock)

    loop.remove_writer.assert_called_once_with(sock)


def test_unexpected_socket_close_removes_reader_and_schedules_reconnect():
    client = make_client()
    loop = make_mock_loop()
    driver = AsyncioMqttDriver(client, loop=loop)
    sock = MagicMock()
    misc = MagicMock()
    driver._misc_task = misc  # pretend the misc loop was running

    driver._on_socket_close(client.mqttc, None, sock)

    loop.remove_reader.assert_called_once_with(sock)
    misc.cancel.assert_called_once()
    assert driver._misc_task is None
    assert any("_reconnect_loop" in n for n in loop.scheduled)


# -- shutdown-race guards (the HIGH finding) ------------------------------------


def test_register_reader_after_stop_closes_sock_and_does_not_arm():
    # A reconnect that lands after stop() must not re-register a reader or leave
    # a live broker socket behind.
    client = make_client()
    loop = make_mock_loop()
    driver = AsyncioMqttDriver(client, loop=loop)
    driver._stopping = True
    sock = MagicMock()

    driver._register_reader(sock)

    loop.add_reader.assert_not_called()
    sock.close.assert_called_once()
    assert driver._sock is None
    assert driver._misc_task is None


def test_add_writer_after_stop_is_noop():
    client = make_client()
    loop = make_mock_loop()
    driver = AsyncioMqttDriver(client, loop=loop)
    driver._stopping = True

    driver._add_writer(MagicMock())

    loop.add_writer.assert_not_called()


def test_stop_waits_for_inflight_connect_before_disconnecting():
    client = make_client()
    driver = AsyncioMqttDriver(client, loop=make_mock_loop())

    async def scenario():
        fut = asyncio.get_running_loop().create_future()
        driver._connect_future = fut  # a blocking (re)connect still in flight
        order = []

        async def stopper():
            await driver.stop()
            order.append("stopped")

        t = asyncio.ensure_future(stopper())
        await asyncio.sleep(0)
        # stop() must be blocked awaiting the in-flight connect, not torn down yet.
        assert order == []
        client.mqttc.disconnect.assert_not_called()

        fut.set_result(None)  # the handshake finishes
        await t
        assert order == ["stopped"]
        client.mqttc.disconnect.assert_called_once()  # disconnect only after it settled

    asyncio.run(scenario())


# -- reconnect scheduling dedup -------------------------------------------------


def test_ensure_reconnect_loop_skips_when_a_task_is_already_running():
    client = make_client()
    loop = make_mock_loop()
    driver = AsyncioMqttDriver(client, loop=loop)
    running = MagicMock()
    running.done.return_value = False
    driver._reconnect_task = running

    driver._ensure_reconnect_loop()

    loop.create_task.assert_not_called()


def test_ensure_reconnect_loop_skips_when_stopping():
    client = make_client()
    loop = make_mock_loop()
    driver = AsyncioMqttDriver(client, loop=loop)
    driver._stopping = True

    driver._ensure_reconnect_loop()

    loop.create_task.assert_not_called()


# -- loop bodies (previously unexecuted) ----------------------------------------


def test_misc_loop_pumps_loop_misc_and_breaks_on_error(monkeypatch):
    client = make_client()
    driver = AsyncioMqttDriver(client, loop=make_mock_loop())
    # SUCCESS, SUCCESS, then a failure code -> break on the third pump.
    client.mqttc.loop_misc.side_effect = [mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_SUCCESS, 1]

    async def fast_sleep(_delay):
        return

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    asyncio.run(driver._misc_loop())

    assert client.mqttc.loop_misc.call_count == 3


def test_reconnect_loop_backs_off_and_returns_on_success(monkeypatch):
    client = make_client()
    driver = AsyncioMqttDriver(client, loop=make_mock_loop())
    slept = []

    async def rec_sleep(delay):
        slept.append(delay)

    monkeypatch.setattr(asyncio, "sleep", rec_sleep)

    calls = {"n": 0}

    async def flaky_connect():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("still down")

    monkeypatch.setattr(driver, "_connect", flaky_connect)
    asyncio.run(driver._reconnect_loop())

    assert calls["n"] == 3
    assert slept == [1.0, 2.0, 4.0]  # exponential backoff before each attempt


# -- stop() ---------------------------------------------------------------------


def test_stop_cancels_tasks_removes_watchers_disconnects_and_closes():
    client = make_client()
    loop = make_mock_loop()
    driver = AsyncioMqttDriver(client, loop=loop)
    sock = MagicMock()
    misc, recon = MagicMock(), MagicMock()
    driver._sock = sock
    driver._misc_task = misc
    driver._reconnect_task = recon

    asyncio.run(driver.stop())

    assert client.is_running is False
    misc.cancel.assert_called_once()
    recon.cancel.assert_called_once()
    client.mqttc.disconnect.assert_called_once()  # inline, best-effort clean close
    client.mqttc.loop_write.assert_called_once()  # flush the queued DISCONNECT
    loop.remove_reader.assert_called_once_with(sock)
    loop.remove_writer.assert_called_once_with(sock)
    sock.close.assert_called_once()
    assert driver._sock is None
    assert driver._stopping is True


def test_stop_is_idempotent_and_handles_no_socket():
    client = make_client()
    loop = make_mock_loop()
    driver = AsyncioMqttDriver(client, loop=loop)

    asyncio.run(driver.stop())
    asyncio.run(driver.stop())  # second call is safe

    assert client.is_running is False
    loop.remove_reader.assert_not_called()  # _sock was never opened


# -- factory + lazy export ------------------------------------------------------


@pytest.mark.filterwarnings("ignore:Callback API version 1 is deprecated")
def test_factory_returns_driver_bound_to_client():
    # Real MqttClient (real paho, hermetic: connect_async needs no broker).
    from ebus_mqtt_client import MqttClient

    client = MqttClient(client_id="probe", endpoint="127.0.0.1", port=1883)
    driver = client.asyncio_driver(loop=make_mock_loop())

    assert isinstance(driver, AsyncioMqttDriver)
    assert driver._client is client
    assert driver._paho is client.mqttc


def test_package_import_does_not_import_asyncio_driver():
    # Fresh interpreter: importing the package must leave asyncio_driver dormant,
    # and accessing the name must load it lazily.
    code = (
        "import sys, ebus_mqtt_client as m\n"
        "assert 'ebus_mqtt_client.asyncio_driver' not in sys.modules\n"
        "d = m.AsyncioMqttDriver\n"
        "assert 'ebus_mqtt_client.asyncio_driver' in sys.modules\n"
        "assert d.__name__ == 'AsyncioMqttDriver'\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_dir_includes_driver_export():
    import ebus_mqtt_client

    assert "AsyncioMqttDriver" in dir(ebus_mqtt_client)
