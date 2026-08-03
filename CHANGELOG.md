# Changelog

All notable changes to `ebus-mqtt-client` are recorded here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). This file was backfilled from git history and the `v0.1.x` tags; consult `git log` and the tags for the underlying commits.

## [Unreleased]

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
