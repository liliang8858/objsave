"""Data models for the ObSave API."""

from typing import Dict, Any, Optional
from pydantic import BaseModel, ConfigDict

class ObjectModel(BaseModel):
    """Base model for objects stored in ObSave."""
    id: str
    name: str
    content_type: str
    size: int
    created_at: str
    model_config = ConfigDict(from_attributes=True)

class JSONObjectModel(BaseModel):
    """Model for JSON objects."""
    id: Optional[str] = None
    type: str
    content: Dict[str, Any]
    name: Optional[str] = None
    content_type: str = "application/json"

class JSONObjectResponse(BaseModel):
    """Response model for JSON objects."""
    id: str
    name: str
    content_type: str
    size: int
    created_at: str
    type: Optional[str] = None
    content: Dict[str, Any]
    model_config = ConfigDict(from_attributes=True)

class JSONQueryModel(BaseModel):
    """Model for JSON object queries."""
    jsonpath: str
    value: Optional[Any] = None
    operator: Optional[str] = 'eq'
    type: Optional[str] = None
    model_config = ConfigDict(from_attributes=True)
