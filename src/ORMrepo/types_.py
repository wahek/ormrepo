# from typing import TypeVar
from typing import Any

from pydantic import BaseModel
from sqlalchemy import UUID
from typing_extensions import TypeVar

from .models import Base

Model = TypeVar("Model", bound=Base)
Schema = TypeVar("Schema", bound=BaseModel)
PK: str | int | UUID | Any | dict[str, Any] | tuple[Any, ...]