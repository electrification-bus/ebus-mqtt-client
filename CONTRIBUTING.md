# Contributing to ebus-mqtt-client

Thanks for your interest in contributing! `ebus-mqtt-client` is a small, standalone MQTT client wrapper around [paho-mqtt](https://pypi.org/project/paho-mqtt/) — TLS, reconnection, subscription recovery, and topic pattern matching. It was extracted from [`ebus-sdk`](https://github.com/electrification-bus/python-sdk) so the MQTT-transport layer could be reused by projects that don't want the rest of the eBus / Homie surface area.

## How to contribute

### Discussions

Use [Discussions](https://github.com/electrification-bus/ebus-mqtt-client/discussions) for:

- Open-ended questions about the library's design, scope, or intent ("would feature X fit here, or does it belong in a wrapper?")
- Proposed new connection / auth shapes (mTLS variants, TLS-PSK, alternative auth backends) — worth aligning on the API before writing the code
- Integration questions ("how do I use this for X?") that aren't yet a clear bug or feature request
- Thinking out loud about a proposed change before scoping it

Discussions are open-ended — a good place to align on direction before something becomes a concrete change. Aligned outcomes often turn into one or more Issues or pull requests.

### Issues

Use [Issues](https://github.com/electrification-bus/ebus-mqtt-client/issues) for actionable changes:

- Bug reports with reproduction steps (broker, paho version, code snippet)
- Concrete feature requests with a clear scope and a use case
- Documentation gaps where a specific README or docstring change is intended
- Discussion outcomes that have alignment and a clear scope

If you're not sure whether something is an Issue or a Discussion, start with a Discussion — we can convert it later.

### Pull requests

Pull requests are welcome.

- For small fixes (typos, docstring tweaks, version bumps, dependency-range adjustments, low-risk bug fixes with a test), open a PR directly.
- For substantive changes (new public API surface, changes to existing API shapes, new dependencies, changes that alter reconnection / subscription-recovery / TLS semantics), open a Discussion or Issue first so we can align on scope before you invest the effort.
- **Stay generic.** This library is intentionally Homie-agnostic and eBus-agnostic — it's just MQTT. If a feature only makes sense in a Homie / eBus context, it belongs in [`ebus-sdk`](https://github.com/electrification-bus/python-sdk) (or a sibling) rather than here. When in doubt, ask in a Discussion.
- **Tests are required.** The existing suite is mock-based (`pytest tests/`) and runs offline in well under a second. New behavior needs a test that exercises it through the mock; new bug fixes need a regression test. If you want to add a real-broker integration test (e.g., a mosquitto fixture), open a Discussion first — that's a meaningful shift in the test infra.
- **Keep comments to a minimum.** The project style is to write self-explanatory code and reserve comments for non-obvious *why* (a hidden constraint, a workaround for a specific bug, a paho quirk). Don't add comments that just restate the code.
- **The version lives in one place.** When a release-worthy change lands, bump `__version__` in `src/ebus_mqtt_client/__init__.py`; that is the single source of truth. `pyproject.toml` reads it dynamically (`dynamic = ["version"]` plus `[tool.setuptools.dynamic]`) and the `setup.py` shim reads the same literal by regex, so neither file carries a `version` value to edit (the shim exists so legacy `setuptools<61`, pinned in Yocto kirkstone, can build a wheel with correct metadata; the docstring at the top of `setup.py` explains this).
- One commit per logical change is fine; we don't require squash or any particular branch naming.

## Releases

Releases to PyPI are automated via the [`Publish to PyPI`](.github/workflows/publish.yml) GitHub Actions workflow, which runs on `v*` git tags using PyPI [trusted publishing](https://docs.pypi.org/trusted-publishers/). Contributors don't need to do anything special — once a maintainer tags `vX.Y.Z`, the workflow tests and publishes.

## Code of conduct

Be respectful and constructive. We appreciate everyone who takes the time to file an issue, start a discussion, or send a pull request.

## Maintenance posture

`ebus-mqtt-client` is an active alpha library. Updates and maintenance, including responses to issues filed on GitHub, will take place on an "as time and resources permit" basis. The library is maintained alongside [`ebus-sdk`](https://github.com/electrification-bus/python-sdk) and the [Electrification Bus specification](https://github.com/electrification-bus/specification) — see the specification repo's README §Governance for the project's long-term governance context.
