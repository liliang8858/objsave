"""Core functionality for ObSave."""

from .storage import ObjectStorage
from .exceptions import ObjectStorageError

__all__ = ["ObjectStorage", "ObjectStorageError"]
