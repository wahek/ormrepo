from typing import Any, TypeAlias, Literal

from pydantic import BaseModel
from sqlalchemy import UUID
from typing_extensions import TypeVar

from .models import OrmBase

ModelT = TypeVar("ModelT", bound=OrmBase) # sqlalchemy model (inherited from DeclarativeBase)
SchemaBaseT = TypeVar("SchemaBaseT", bound=BaseModel) # pydantic model
SchemaT = TypeVar("SchemaT", bound=BaseModel)
PK: TypeAlias = str | int | UUID | Any | dict[str, Any] | tuple[Any, ...] # any primary key (composite or regular)
log_level = Literal['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'] # logging level