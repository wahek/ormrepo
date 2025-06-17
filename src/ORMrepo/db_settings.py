import logging
from typing import Any

from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)


class ConfigORM(BaseModel, validate_assignment=True):
    limit: int = Field(gt=0, default=1000)
    global_filters: dict[str, Any] = None


config_orm = ConfigORM()
config_orm.global_filters = {'id': 1}
