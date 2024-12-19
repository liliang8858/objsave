from fastapi import HTTPException
from typing import Any, Optional

class ObjSaveException(HTTPException):
    def __init__(
        self,
        status_code: int,
        detail: Any = None,
        headers: Optional[dict] = None,
        error_code: str = None
    ):
        super().__init__(status_code=status_code, detail=detail, headers=headers)
        self.error_code = error_code

class ObjectNotFoundError(ObjSaveException):
    def __init__(self, object_id: str):
        super().__init__(
            status_code=404,
            detail=f"Object {object_id} not found",
            error_code="OBJECT_NOT_FOUND"
        )

class InvalidJSONPathError(ObjSaveException):
    def __init__(self, path: str):
        super().__init__(
            status_code=400,
            detail=f"Invalid JSONPath expression: {path}",
            error_code="INVALID_JSONPATH"
        )

class StorageError(ObjSaveException):
    def __init__(self, message: str):
        super().__init__(
            status_code=500,
            detail=f"Storage error: {message}",
            error_code="STORAGE_ERROR"
        )
