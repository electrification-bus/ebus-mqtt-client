"""Legacy setup.py shim for setuptools < 61 (PEP 621 pre-support).

Modern setuptools (>=61) reads all package metadata from pyproject.toml's
[project] table and ignores the args passed here. The explicit name/version/
packages are needed only so that older setuptools (e.g. the 59.5.0 pinned in
Yocto kirkstone) can build a wheel with correct metadata from the sdist —
without this shim the legacy build produces an UNKNOWN-0.0.0 wheel.

Keep name and version in sync with pyproject.toml [project].
"""

from setuptools import setup

setup(
    name="ebus-mqtt-client",
    version="0.1.4",
    package_dir={"": "src"},
    packages=["ebus_mqtt_client"],
)
