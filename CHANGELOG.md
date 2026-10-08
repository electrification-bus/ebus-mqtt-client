# Changelog

All notable changes to `ebus-mqtt-client` are recorded here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). This file was backfilled from git history and the `v0.1.x` tags; consult `git log` and the tags for the underlying commits.

## [Unreleased]

### Added

- `subscribe()` takes a keyword-only `with_retain` flag ([#24](https://github.com/electrification-bus/ebus-mqtt-client/issues/24)). With `with_retain=True` the subscription's callback receives `(topic, payload, retained)`, where `retained` is paho's `msg.retain` as a `bool`, and a constructor `callback` receives `(topic, payload, param, retained)`. Under MQTT 3.1.1 the broker sets the flag only when it replays a stored retained message to a new subscription, so a subscriber can tell that replay from a live message. The flag is per subscription and is kept by subscription recovery on reconnect. Subscriptions made without it are delivered as before. A delivery carries no record of which subscription caused it and is routed to one matching filter only, so with overlapping filters `retained=True` also marks a replay caused by subscribing any overlapping filter; `subscribe()` logs a warning when a `with_retain` filter overlaps another.

## [0.6.0] - 2026-10-03

### Fixed

- Retained state published while the link is down no longer ends on a stale or dropped value at QoS 1 and 2 ([#20](https://github.com/electrification-bus/ebus-mqtt-client/issues/20)). Such a publish was handed to paho, which stores it and on CONNACK replays its whole queue in one burst that ignores `max_inflight_messages` and lands after `on_connect`. At QoS 1 and 2 a value republished from `on_connect_callback` was then overwritten by paho's older copy. At QoS 2 the burst could also exceed mosquitto's per-client receive quota (`max_inflight_messages`, default 20), and for an MQTT 3.1.1 client mosquitto acknowledges the excess and discards it, so whichever value fell past the quota was lost without any error on either side. A device that builds its tree before the connection is up hit both: with ebus-sdk's `simple-device` and `simple-tree-device` examples the root's retained `$state` ended on `init` instead of `ready`.

  The wrapper now never hands paho a publish while the link is down. Retained publishes at every QoS are held newest-value-per-topic, as QoS 0 already was; non-retained QoS 1 and 2 publishes are held in order, one entry each; non-retained QoS 0 is still dropped. The link counts as down from the disconnect until the flush on the next connect takes the hold, so a publish issued between CONNACK and the flush joins the hold instead of overtaking it. The hold is flushed before `on_connect_callback`, on a live link where paho's inflight cap applies; a retained value the callback republishes wins over a flushed value for the same topic at the same or a higher QoS (a broker may apply QoS 2 only at PUBREL, as mosquitto does, so a lower-QoS republish can be overtaken). `pending_limit` now bounds every held entry, not only QoS 0 retained topics. Nothing is held between `stop()` (the client's or the asyncio driver's) and the next start. Arguments paho rejects (a non-`str` or wildcard topic, an empty topic under MQTT 3.1.1, an invalid QoS, an unencodable payload) still raise from `publish()` while the link is down, as they did when such a publish went to paho. A held publish still returns an `MQTTMessageInfo` with `rc == MQTT_ERR_NO_CONN`.

  Not covered: what paho had accepted on a live link and not finished delivering when the link dropped is still replayed by paho, after `on_connect` and uncapped ([#21](https://github.com/electrification-bus/ebus-mqtt-client/issues/21)).

- `publish_and_flush()` is serialized against the flush, so a final retained value (ebus-sdk publishes its graceful `$state` this way) can no longer land ahead of an older held value for the same topic and be overwritten by it. It returns `False` while the link is down, including between CONNACK and the flush.
- A publish from `on_disconnect_callback` can no longer deadlock against another thread's publish. paho can invoke the callback while holding its own out-queue mutex; such a call now reads the link as down without taking the lock a publishing thread holds across its call into paho.
- With `v5=True`, connecting no longer raises `TypeError`. paho passes MQTTv5 `on_connect` and `on_disconnect` callbacks a trailing `properties` argument that the internal handlers did not accept, so CONNACK raised in the network loop, `on_connect_callback` never ran, and subscriptions were not recovered.

### Changed

- A refused CONNACK (nonzero result code) no longer flushes the hold, recovers subscriptions, or invokes `on_connect_callback`; those now run only on a successful connect. Flushing on a refused connection would mark the link ready and send publishes to paho while it is down.

### Added

- `tests/test_broker.py`: tests against a real mosquitto, started per test on a free port and skipped when no `mosquitto` binary is found; CI installs mosquitto so they run there. These fail on 0.5.0: the pre-connect burst past the broker quota, the `on_connect` republish overwritten by an older value, a publish between CONNACK and the flush, MQTTv5 connect, and a publish after the asyncio driver stops. The file also runs mTLS handshakes against a broker that requires client certificates: certificate and key paths, in-memory certificate and key, a password-protected key, refusal of a client without a certificate, and refusal of a server certificate from an untrusted CA.

## [0.5.0] - 2026-08-21

### Fixed

- Publishing before the connection is up no longer logs a warning, and a QoS 0 retained message issued then is no longer lost. `__init__` registers the broker with `connect_async`, so CONNACK does not arrive until the network loop started by `start()` receives it, and paho refuses every publish issued in between with `MQTT_ERR_NO_CONN`; `publish()` treated that as a fault and warned on each one. For a caller that announces retained state at startup that is a warning per topic on every start, costing journald budget on embedded targets and reading as a broker or auth failure to anyone debugging one. Such a publish now logs a single debug line recording what became of the message, and every other publish failure still warns, now reporting the paho result code.

  What becomes of the message depends on its QoS, because paho only discards some of them. At QoS 1 and 2 (the `publish()` default is QoS 1) paho stores the message in its own out-queue before returning `MQTT_ERR_NO_CONN`, keeps it across reconnects, and re-sends it once CONNACK arrives, so it is left alone. At QoS 0 paho keeps nothing: a retained message is therefore held here and flushed on connect (before subscription recovery and before `on_connect_callback`, so anything the callback publishes is newer and lands after), and a non-retained one is dropped, because it is an event and delivering it after an arbitrary delay announces something that was true once.

  The hold keeps the newest value per topic rather than replaying every attempt in order: retained state is last-value-wins, so a plain queue could write a stale value on top of a newer one published after the connection came up, leaving a device permanently announcing a state it had already left. It is serialised against the flush with a lock, because the flush runs on paho's network thread and the same overwrite is otherwise reachable by a caller publishing concurrently with it. It is bounded by the new per-instance `pending_limit` (default 512 topics, evicting the oldest), with an overflow reported once and then sampled rather than once per drop. A publish refused again mid-flush (the link dropped partway through the drain) goes back into the hold rather than being lost. `stop()` discards anything still held.

  `publish()` still returns paho's `MQTTMessageInfo` unchanged, so a publish refused this way reports `rc == MQTT_ERR_NO_CONN` to the caller whichever branch it took. `publish_and_flush()` was never affected (it checks `is_connected()` and returns `False` early) and keeps its warning: unlike the startup transient, its callers use it to confirm a final message reached the broker, where not-connected genuinely means the message did not land.

## [0.4.0] - 2026-08-03

### Added

- Optional loop-native (asyncio) transport driver `AsyncioMqttDriver` (new `ebus_mqtt_client.asyncio_driver` module) plus a lazy `MqttClient.asyncio_driver()` factory. It pumps an `MqttClient`'s paho network loop on a caller-supplied asyncio event loop (paho socket hooks plus a periodic `loop_misc`) instead of paho's background thread, so a host that already owns an event loop (for example Home Assistant) can run all MQTT I/O on its own loop and inject the client into `ebus_sdk.Controller(mqttc=...)`. Purely additive: no change to `start()` / `stop()` / `from_config()` / `publish` / `subscribe` or the existing callbacks, and thread mode and the driver are mutually exclusive per instance (chosen by the caller). The driver module is not imported when the package is imported (it loads lazily via a module `__getattr__` and the factory), imports only the standard library plus paho, and works across `paho-mqtt>=1.5.0`, so a thread-only consumer (and a constrained/Yocto panel build) never loads it.

## [0.3.0] - 2026-08-02

### Added

- PEP 561 `py.typed` marker: the package now ships its inline type information, so downstream type checkers resolve `MqttClient` to the concrete class instead of `Any`. The marker is wired into both the modern build (`[tool.setuptools.package-data]`) and the legacy `setup.py` shim (`package_data`, for the Yocto/kirkstone path where a bare marker is otherwise dropped); the built wheel and sdist both contain it.

## [0.2.0] - 2026-08-02

### Added

- `on_disconnect_callback` constructor parameter (and matching `from_config` parameter) on `MqttClient`, mirroring the existing `on_connect_callback`. It is invoked from the internal disconnect handler and receives paho's reason code as its single argument, so a caller can react to a dropped or clean connection (for example to mirror connection state into a health readout) instead of polling `is_connected()`. Invoked best-effort: a consumer exception is logged (`reason=onDisconnectCallbackException`) and swallowed so it cannot disrupt paho's network loop. Backward compatible: omitting it preserves current behavior.
- Static type checking with mypy: a `[tool.mypy]` config, `mypy` in the `dev` extra, and a `mypy` job in the `lint.yml` CI workflow. The package now type-checks clean. paho-mqtt is treated as an opaque (untyped, `Any`) dependency via a `paho.*` override (`follow_imports = "skip"`), independent of the installed paho major: 1.x ships no type information, and 2.x ships types for a different callback-API surface than this v1-targeting wrapper uses. This keeps the mypy result stable across `paho-mqtt>=1.5.0`.

### Fixed

- mTLS client-certificate loading no longer raises during construction when a client key is supplied without a client certificate. Such a pair cannot form a certificate chain, so it is now logged (`reason=mqttClientTlsClientKeyWithoutCert`) and skipped, instead of calling `load_cert_chain` with a missing `certfile` (which raised `TypeError`). Server-only TLS and normal client cert + key mTLS are unaffected.

### Changed

- Internal type-annotation hygiene (no behavior change): explicit `Optional` on the `callback` parameters of `__init__` and `from_config`, a type annotation on the `sub_callbacks` dict, and `publish_and_flush` now coerces paho's `is_published()` result to `bool` so its declared `-> bool` return type holds.

## [0.1.8] - 2026-07-20

### Fixed

- Resilient broker connect: construction is no longer coupled to broker availability. `MqttClient.__init__` now calls paho's `connect_async()` (non-blocking, never raising on a down or unreachable broker) instead of the synchronous `connect()`. The real TCP/MQTT connect runs on the network thread started by `start()` -> `loop_start()`, which retries the first connection using the existing reconnect backoff until the broker becomes reachable. Previously a broker that was briefly unavailable at construction time (startup, restart, network blip) made the constructor raise `ConnectionRefusedError`, leaving callers with a silent, never-connecting "zombie" publisher whose every publish was a no-op (`reconnect_delay_set` only governs post-first-connect reconnects, so it never helped a never-connected client). The `connect_async()` call is additionally guarded so construction stays exception-free even on bad connection parameters. Behavioral note for callers: `is_connected()` returns `False` between construction and the first successful connect on the network loop; gate publishes on it (or use `publish_and_flush`, which already checks) if you must not publish before the link is up. A briefly-unavailable broker at startup, restart, or a transient network blip is a normal MQTT condition, so tolerating it at construction time benefits any consumer.

### Changed

- Adopted the eBus "version single source of truth" convention: `__version__` in `src/ebus_mqtt_client/__init__.py` is now the one place the version is written (and is importable at runtime). `pyproject.toml` resolves it dynamically (`dynamic = ["version"]` + `[tool.setuptools.dynamic]`), the `setup.py` legacy shim reads it by regex instead of a hardcoded literal, and the publish workflow gained a "Verify tag matches package version" guard that fails a release whose `v*` tag disagrees with `__version__`. A `## Releasing` section documenting the flow was added to the README.

## [0.1.7] - 2026-07-11

### Added

- `MqttClient.publish_and_flush(topic, data, qos=1, retain=False, timeout=1.0) -> bool`: publish a message and bounded-wait until it is actually sent to the broker. Returns `True` once flushed; returns `False` immediately (never raising, never blocking indefinitely) when there is no client, the client is not connected, the publish result code is a failure, or the flush does not complete within `timeout`. Lets a caller land a final retained message (for example a graceful state update) before a clean disconnect without a fixed sleep.
- Ruff formatting and linting: a `[tool.ruff]` config (line-length 100, target py310, the E/W/F/I/B/UP/SIM lint set), a `ruff-pre-commit` hook, and a `lint.yml` CI job running `ruff check` and `ruff format --check`.
- PyPI version and Ruff badges in the README.

### Changed

- `MqttClient.publish()` now returns paho's `MQTTMessageInfo` (or `None` when there is no client) instead of discarding it, so a caller can `wait_for_publish(timeout)` for a bounded flush. Backward compatible: callers that ignore the return value are unaffected.
- `MqttClient.stop()` is now bounded and broker-independent, taking a `timeout` (default 2.0s). It runs the potentially-blocking `disconnect()` + `loop_stop()` in a daemon helper thread and joins only for `timeout`, so a dead or unreachable broker can no longer stall the caller (previously `loop_stop()` joined the paho network thread with no timeout). The clean DISCONNECT is best-effort and never depended on; shutdown falls back to the daemon thread plus the LWT.
- Applied an initial Ruff format and lint cleanup across `src` and `tests` (no behavior change): import sorting, PEP 604 unions and `collections.abc.Callable`, `except Exception:` in place of bare `except:`, `contextlib.suppress`, and dropping a mutable default argument.

## [0.1.6] - 2026-06-13

### Added

- `MqttClient.unsubscribe(sub)`: remove a subscription's local callback and matcher entry (so a later re-publish will not dispatch and the on-reconnect recovery path will not re-subscribe it) and send UNSUBSCRIBE to the broker. Returns `True` when the filter was known, `False` otherwise.
- `CONTRIBUTING.md`, linked from the README.

### Changed

- Bumped the publish workflow's GitHub Actions to Node 24-compatible versions.

## [0.1.5] - 2026-06-08

### Added

- mTLS client certificate/key configuration for `MqttClient`: `tls_client_cert` / `tls_client_cert_data`, `tls_client_key` / `tls_client_key_data`, and `tls_client_key_password` (with matching `from_config` keys). In-memory PEM data is materialised to a 0600 temp file for the `load_cert_chain` call and unlinked afterward; the `*_data` form takes precedence over the file-path form.

## [0.1.4] - 2026-05-12

### Added

- A `setup.py` shim for compatibility with legacy setuptools that cannot build from `pyproject.toml` alone.

## [0.1.3] - 2026-05-06

### Changed

- Switched the build backend to `setuptools.build_meta` (from hatch).
- Relaxed the `paho-mqtt` dependency floor to `>=1.5.0` for Yocto compatibility.

## [0.1.1] - 2026-03-21

Initial standalone release, extracted from `ebus-sdk` (extraction commits dated 2026-03-14 predate the first tag).

### Added

- `MqttClient`: a wrapper around paho-mqtt v2 providing TLS (secure with CA verification, insecure, or plaintext), automatic reconnection with configurable backoff, subscription recovery on reconnect, topic pattern matching via paho's `MQTTMatcher`, Last Will and Testament (LWT), MQTTv3 and MQTTv5 support, and a `from_config` dict factory.
- `AUTH_TYPE_USER_PASS` constant and username/password authentication.
- MIT `LICENSE` and package metadata.
- PyPI trusted-publishing workflow.
