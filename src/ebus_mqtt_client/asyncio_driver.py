"""Optional loop-native (asyncio) driver for :class:`MqttClient`.

By default :meth:`MqttClient.start` spawns a paho background network thread
(``loop_start``). This module offers an alternative: drive the same paho network
loop on a caller-supplied asyncio event loop, using paho's socket hooks
(``on_socket_open`` / ``on_socket_register_write`` plus a periodic
``loop_misc``). All MQTT I/O then runs on the consumer's event loop with no
extra threads, which is useful for hosts that already own an event loop (for
example Home Assistant) and want to inject the client into
``ebus_sdk.Controller(mqttc=...)``.

The module imports only the standard library plus paho, and it is NOT imported
when the package is imported: it loads lazily via ``ebus_mqtt_client``'s module
``__getattr__`` (or :meth:`MqttClient.asyncio_driver`), so a thread-mode
consumer never pays for it.

This is additive. It reads ``client.mqttc`` and flips the public
``client.is_running`` flag, and it claims paho's ``on_socket_*`` hooks, which
:meth:`MqttClient.__init__` does not wire (it wires ``on_connect`` /
``on_disconnect`` / ``on_message``). ``MqttClient``'s existing methods and
runtime behaviour are unchanged; ``client.py`` gains only the optional
``asyncio_driver()`` factory.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

import paho.mqtt.client as mqtt

if TYPE_CHECKING:
    import concurrent.futures

    from ebus_mqtt_client.client import MqttClient

_LOGGER = logging.getLogger(__name__)


class AsyncioMqttDriver:
    """Pump an :class:`MqttClient`'s paho network loop on an asyncio event loop.

    Optional alternative to :meth:`MqttClient.start` (which spawns a paho
    background thread). The blocking TCP+TLS (re)connect handshake runs in an
    executor; the ongoing ``loop_read`` / ``loop_write`` / ``loop_misc`` run on
    the event loop. An unexpected drop is retried in the executor with backoff.

    Construct within a running event loop, or pass ``loop`` explicitly.

    Notes:

    1. Thread mode (:meth:`MqttClient.start`) and this driver are mutually
       exclusive per :class:`MqttClient` instance. Pick one; do not call
       ``start()`` on a client this driver manages. ``start()`` is single-use
       per driver (it reconnects internally); construct a new driver to restart.
    2. paho's automatic reconnect (``reconnect_delay_set``, set in
       ``MqttClient.__init__``) is INERT under loop-native driving: paho's
       auto-reconnect only runs inside ``loop()`` / ``loop_forever()`` /
       ``loop_start()``. That is why this driver implements its own backoff
       reconnect loop.
    3. Bring-your-own-transport consumers of ``ebus_sdk.Controller``: a
       tree-rooted ``Controller`` caller must wire ``Controller.resync`` onto
       the client's on-connect callback
       (``client.on_connect_callback = controller.resync``) so the retained
       tree re-walks after a broker reconnect. The SDK auto-wires this only for
       a client it OWNS; this driver has no ``Controller`` handle and cannot do
       it for you.
    """

    def __init__(
        self,
        client: MqttClient,
        loop: asyncio.AbstractEventLoop | None = None,
        executor: concurrent.futures.Executor | None = None,
    ) -> None:
        """Bind the driver to ``client``.

        ``loop`` defaults to :func:`asyncio.get_running_loop`, so construct this
        from within a running event loop or pass one explicitly. ``executor``
        (default: the loop's default executor) runs the blocking paho
        (re)connect handshake off the loop.
        """
        self._client = client
        self._paho = client.mqttc
        self._loop = loop if loop is not None else asyncio.get_running_loop()
        self._executor = executor
        self._misc_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._connect_future: asyncio.Future[Any] | None = None
        self._sock: Any = None
        self._started = False
        self._stopping = False

    async def start(self) -> None:
        """Claim paho's socket hooks and open the connection.

        Single-use: call once per driver. The driver reconnects internally, so a
        stopped driver is not restarted; construct a new one instead (a second
        ``start()`` raises ``RuntimeError``). Mirrors ``MqttClient.start``'s
        connect resilience: a broker that is momentarily down or refuses the
        connect must not raise here. The blocking handshake runs in the executor;
        on failure the backoff reconnect loop is scheduled and ``start`` returns.
        """
        if self._started:
            raise RuntimeError(
                "AsyncioMqttDriver.start() already called; construct a new driver to restart"
            )
        self._started = True
        # paho drives these on socket lifecycle events; they may fire on the
        # executor thread during a (re)connect, so every loop mutation is
        # bounced onto the loop thread with call_soon_threadsafe. The set is
        # disjoint from MqttClient's on_connect/on_disconnect/on_message.
        self._paho.on_socket_open = self._on_socket_open
        self._paho.on_socket_close = self._on_socket_close
        self._paho.on_socket_register_write = self._on_socket_register_write
        self._paho.on_socket_unregister_write = self._on_socket_unregister_write

        self._client.is_running = True
        try:
            await self._connect()
        except Exception as err:  # noqa: BLE001 - any first-connect failure retries in the loop
            _LOGGER.debug("reason=asyncioMqttInitialConnectFailed,err=%s,retry=background", err)
            self._ensure_reconnect_loop()

    async def _connect(self) -> None:
        # reconnect() uses the connect_async target MqttClient set at
        # construction and performs the blocking handshake, so run it off-loop.
        # Track the future so stop() can wait it out and never race teardown.
        # shield() so that cancelling the reconnect loop (which stop() does)
        # does NOT cancel the tracked future: the executor thread cannot be
        # cancelled anyway, and stop() must be able to await it to completion.
        self._connect_future = self._loop.run_in_executor(self._executor, self._paho.reconnect)
        await asyncio.shield(self._connect_future)

    def _ensure_reconnect_loop(self) -> None:
        """Start the backoff reconnect loop unless one is running or stopping."""
        if not self._stopping and (self._reconnect_task is None or self._reconnect_task.done()):
            self._reconnect_task = self._loop.create_task(self._reconnect_loop())

    # -- paho socket hooks (may run on the executor thread) --------------------

    def _on_socket_open(self, client: Any, userdata: Any, sock: Any) -> None:
        self._loop.call_soon_threadsafe(self._register_reader, sock)

    def _register_reader(self, sock: Any) -> None:
        # A (re)connect that completed after stop() must not re-arm the loop or
        # leave a live broker socket behind; close it and bail.
        if self._stopping:
            with contextlib.suppress(Exception):
                sock.close()
            return
        self._sock = sock
        self._loop.add_reader(sock, self._paho.loop_read)
        if self._misc_task is None or self._misc_task.done():
            self._misc_task = self._loop.create_task(self._misc_loop())

    def _on_socket_close(self, client: Any, userdata: Any, sock: Any) -> None:
        self._loop.call_soon_threadsafe(self._unregister_reader, sock)

    def _unregister_reader(self, sock: Any) -> None:
        with contextlib.suppress(Exception):
            self._loop.remove_reader(sock)
        if self._misc_task is not None:
            self._misc_task.cancel()
            self._misc_task = None
        # Unexpected drop: schedule an executor reconnect with backoff.
        self._ensure_reconnect_loop()

    def _on_socket_register_write(self, client: Any, userdata: Any, sock: Any) -> None:
        self._loop.call_soon_threadsafe(self._add_writer, sock)

    def _add_writer(self, sock: Any) -> None:
        if self._stopping:
            return
        self._loop.add_writer(sock, self._paho.loop_write)

    def _on_socket_unregister_write(self, client: Any, userdata: Any, sock: Any) -> None:
        self._loop.call_soon_threadsafe(self._remove_writer, sock)

    def _remove_writer(self, sock: Any) -> None:
        with contextlib.suppress(Exception):
            self._loop.remove_writer(sock)

    # -- loop tasks ------------------------------------------------------------

    async def _misc_loop(self) -> None:
        """Drive paho keepalive/ping bookkeeping on the loop."""
        with contextlib.suppress(asyncio.CancelledError):
            while not self._stopping:
                await asyncio.sleep(1)
                if self._paho.loop_misc() != mqtt.MQTT_ERR_SUCCESS:
                    break

    async def _reconnect_loop(self) -> None:
        """Retry the blocking (re)connect in the executor, backing off to 30s."""
        delay = 1.0
        while not self._stopping:
            await asyncio.sleep(delay)
            if self._stopping:
                return
            try:
                await self._connect()
                return  # on_socket_open re-registers the reader + restarts misc
            except Exception:  # noqa: BLE001 - any connect failure just retries
                _LOGGER.debug("reason=asyncioMqttReconnectFailed,retry=1")
                delay = min(delay * 2, 30.0)

    async def stop(self) -> None:
        """Cancel loop tasks and tear the connection down. Safe to call twice.

        The caller owns the client's lifecycle here (this driver never runs
        under an SDK-owned client). Shutdown is deterministic: any in-flight
        blocking (re)connect is awaited to completion before teardown (a thread
        is not cancellable), and the ``_stopping`` guard in the socket hooks
        neutralises a connect that lands late. A clean DISCONNECT is
        best-effort: it is queued and flushed inline on the loop, and the LWT
        remains the reliable teardown signal if the flush does not land.
        """
        self._stopping = True
        self._client.is_running = False
        for task in (self._misc_task, self._reconnect_task):
            if task is not None:
                task.cancel()
        self._misc_task = None
        self._reconnect_task = None
        # Wait out any in-flight blocking (re)connect running in the executor so
        # it cannot race the teardown below; its on_socket_open is already
        # neutralised by the _stopping guard in _register_reader.
        if self._connect_future is not None:
            await asyncio.gather(self._connect_future, return_exceptions=True)
            self._connect_future = None
        # Best-effort clean DISCONNECT: queue it and flush inline on the loop
        # (both non-blocking in loop-native mode). No executor, so this cannot
        # overlap the now-settled reconnect above.
        with contextlib.suppress(Exception):
            self._paho.disconnect()
            self._paho.loop_write()
        # Drop the loop I/O watchers and close the socket (paho will not reuse it
        # after disconnect); each step is safe if it is already gone.
        if self._sock is not None:
            for remove in (self._loop.remove_reader, self._loop.remove_writer):
                with contextlib.suppress(Exception):
                    remove(self._sock)
            with contextlib.suppress(Exception):
                self._sock.close()
            self._sock = None
