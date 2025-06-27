from typing import Any, TypeAlias, Literal

from pydantic import BaseModel
from sqlalchemy import UUID
from typing_extensions import TypeVar

from .models import OrmBase

Model = TypeVar("Model", bound=OrmBase) # sqlalchemy model (inherited from DeclarativeBase)
Schema = TypeVar("Schema", bound=BaseModel) # pydantic model
PK: TypeAlias = str | int | UUID | Any | dict[str, Any] | tuple[Any, ...] # any primary key (composite or regular)
log_level = Literal['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'] # logging level