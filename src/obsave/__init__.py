"""
ObSave - 高性能对象存储服务
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ObSave is a high-performance object storage service built on FastAPI.
Basic usage:

   >>> from obsave import ObjectStorage
   >>> storage = ObjectStorage()
   >>> storage.store("my_key", {"data": "value"})
   >>> obj = storage.get("my_key")
   >>> print(obj)
   {"data": "value"}

:copyright: (c) 2024 by ObSave Team.
:license: MIT, see LICENSE for more details.
"""

__version__ = "1.0.0"
__author__ = "ObSave Team"
__license__ = "MIT"

from obsave.core.storage import ObjectStorage
from obsave.core.exceptions import StorageError, ObjectNotFoundError

__all__ = [
    'ObjectStorage',
    'StorageError',
    'ObjectNotFoundError',
    '__version__'
]
