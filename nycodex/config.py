import os

__all__ = ["DATABASE_URI", "LOG_LEVEL"]

DATABASE_URI = os.environ["DATABASE_URI"]

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
assert LOG_LEVEL in {"DEBUG", "INFO", "WARNING"}  # Can't disable WARNINGs
