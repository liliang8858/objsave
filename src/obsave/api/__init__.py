"""API endpoints and routing for ObSave."""

from .routes import router
from .models import ObjectModel

__all__ = ["router", "ObjectModel"]
