from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("event-search")
except PackageNotFoundError:
    __version__ = "unknown"


__all__ = ["__version__"]
