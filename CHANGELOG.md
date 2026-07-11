# Changelog

All notable changes to `ebus-mqtt-client` are recorded here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). This file was backfilled from git history and the `v0.1.x` tags; consult `git log` and the tags for the underlying commits.

## [Unreleased]

## [0.1.7] - 2026-07-11

### Added

- `MqttClient.publish_and_flush(topic, data, qos=1, retain=False, timeout=1.0) -> bool`: publish a message and bounded-wait until it is actually sent to the broker. Returns `True` once flushed; returns `False` immediately (never raising, never blocking indefinitely) when there is no client, the client is not connected, the publish result code is a failure, or the flush does not complete within `timeout`. Lets a caller land a final retained message (for example a graceful state update) before a clean disconnect without a fixed sleep. (EMQTT-yn2)
- Ruff formatting and linting: a `[tool.ruff]` config (line-length 100, target py310, the E/W/F/I/B/UP/SIM lint set), a `ruff-pre-commit` hook, and a `lint.yml` CI job running `ruff check` and `ruff format --check`. (EMQTT-73n)
- PyPI version and Ruff badges in the README. (EMQTT-9b3)

### Changed

- `MqttClient.publish()` now returns paho's `MQTTMessageInfo` (or `None` when there is no client) instead of discarding it, so a caller can `wait_for_publish(timeout)` for a bounded flush. Backward compatible: callers that ignore the return value are unaffected. (EMQTT-yn2)
- `MqttClient.stop()` is now bounded and broker-independent, taking a `timeout` (default 2.0s). It runs the potentially-blocking `disconnect()` + `loop_stop()` in a daemon helper thread and joins only for `timeout`, so a dead or unreachable broker can no longer stall the caller (previously `loop_stop()` joined the paho network thread with no timeout). The clean DISCONNECT is best-effort and never depended on; shutdown falls back to the daemon thread plus the LWT. (EMQTT-lk7)
- Applied an initial Ruff format and lint cleanup across `src` and `tests` (no behavior change): import sorting, PEP 604 unions and `collections.abc.Callable`, `except Exception:` in place of bare `except:`, `contextlib.suppress`, and dropping a mutable default argument. (EMQTT-73n)

## [0.1.6] - 2026-06-13

### Added

- `MqttClient.unsubscribe(sub)`: remove a subscription's local callback and matcher entry (so a later re-publish will not dispatch and the on-reconnect recovery path will not re-subscribe it) and send UNSUBSCRIBE to the broker. Returns `True` when the filter was known, `False` otherwise. (EMQTT-4p1)
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
