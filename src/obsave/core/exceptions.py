from fastapi import HTTPException
from typing import Any, Optional, Dict

class ObjSaveException(HTTPException):
    """基础异常类"""
    def __init__(
        self,
        status_code: int,
        detail: Any = None,
        headers: Optional[Dict[str, str]] = None,
        error_code: str = None,
        error_data: Optional[Dict[str, Any]] = None
    ):
        super().__init__(status_code=status_code, detail=detail, headers=headers)
        self.error_code = error_code
        self.error_data = error_data or {}

class ObjectNotFoundError(ObjSaveException):
    """对象未找到异常"""
    def __init__(self, object_id: str):
        super().__init__(
            status_code=404,
            detail=f"Object {object_id} not found",
            error_code="OBJECT_NOT_FOUND",
            error_data={"object_id": object_id}
        )

class InvalidJSONPathError(ObjSaveException):
    """无效的 JSONPath 表达式异常"""
    def __init__(self, path: str, reason: str = None):
        super().__init__(
            status_code=400,
            detail=f"Invalid JSONPath expression: {path}" + (f" - {reason}" if reason else ""),
            error_code="INVALID_JSONPATH",
            error_data={"path": path, "reason": reason}
        )

class StorageError(ObjSaveException):
    """存储错误基类"""
    def __init__(self, message: str, error_data: Optional[Dict[str, Any]] = None):
        super().__init__(
            status_code=500,
            detail=f"Storage error: {message}",
            error_code="STORAGE_ERROR",
            error_data=error_data
        )

class StorageIOError(StorageError):
    """存储 IO 错误"""
    def __init__(self, path: str, operation: str, original_error: Exception):
        super().__init__(
            message=f"IO error during {operation} operation on {path}",
            error_data={
                "path": path,
                "operation": operation,
                "error_type": type(original_error).__name__,
                "error_message": str(original_error)
            }
        )
        self.error_code = "STORAGE_IO_ERROR"

class StorageQuotaExceededError(StorageError):
    """存储配额超限异常"""
    def __init__(self, current_size: int, max_size: int):
        super().__init__(
            message=f"Storage quota exceeded. Current: {current_size}B, Max: {max_size}B",
            error_data={
                "current_size": current_size,
                "max_size": max_size,
                "exceeded_by": current_size - max_size
            }
        )
        self.error_code = "STORAGE_QUOTA_EXCEEDED"

class ConcurrencyError(ObjSaveException):
    """并发错误"""
    def __init__(self, operation: str, max_concurrent: int):
        super().__init__(
            status_code=429,
            detail=f"Too many concurrent {operation} operations (max: {max_concurrent})",
            error_code="CONCURRENCY_LIMIT_EXCEEDED",
            error_data={
                "operation": operation,
                "max_concurrent": max_concurrent
            }
        )

class ValidationError(ObjSaveException):
    """数据验证错误"""
    def __init__(self, field: str, reason: str):
        super().__init__(
            status_code=400,
            detail=f"Validation error for field '{field}': {reason}",
            error_code="VALIDATION_ERROR",
            error_data={
                "field": field,
                "reason": reason
            }
        )

class ConfigurationError(ObjSaveException):
    """配置错误"""
    def __init__(self, parameter: str, reason: str):
        super().__init__(
            status_code=500,
            detail=f"Configuration error for '{parameter}': {reason}",
            error_code="CONFIGURATION_ERROR",
            error_data={
                "parameter": parameter,
                "reason": reason
            }
        )

class ResourceExhaustedError(ObjSaveException):
    """资源耗尽错误"""
    def __init__(self, resource: str, limit: Any, current: Any):
        super().__init__(
            status_code=429,
            detail=f"Resource exhausted: {resource} (limit: {limit}, current: {current})",
            error_code="RESOURCE_EXHAUSTED",
            error_data={
                "resource": resource,
                "limit": limit,
                "current": current
            }
        )

class RetryExhaustedError(StorageError):
    """重试次数耗尽错误"""
    def __init__(self, operation: str, attempts: int, last_error: Exception):
        super().__init__(
            message=f"Operation '{operation}' failed after {attempts} attempts",
            error_data={
                "operation": operation,
                "attempts": attempts,
                "last_error_type": type(last_error).__name__,
                "last_error_message": str(last_error)
            }
        )
        self.error_code = "RETRY_EXHAUSTED"
