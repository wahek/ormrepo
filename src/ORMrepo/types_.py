# from typing import TypeVar
from typing import Any, TypeAlias, Literal

from pydantic import BaseModel
from sqlalchemy import UUID
from typing_extensions import TypeVar

from .models import OrmBase

Model = TypeVar("Model", bound=OrmBase)
Schema = TypeVar("Schema", bound=BaseModel)
PK: TypeAlias = str | int | UUID | Any | dict[str, Any] | tuple[Any, ...]
orm_rep_kwargs: TypeAlias = Literal['use_global_filters']