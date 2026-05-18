"""Sentinel — AI on-call copilot."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("sentinel")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"
