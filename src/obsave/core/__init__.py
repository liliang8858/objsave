"""ObSave core module."""

from .storage import ObjectStorage
from .exceptions import StorageError, ObjectNotFoundError

__all__ = ['ObjectStorage', 'StorageError', 'ObjectNotFoundError']
