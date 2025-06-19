from typing import Any

from .exceptions import ConfigurateException


class ConfigORM:
    def __init__(self, limit: int = 1000, global_filters: dict[str, Any] = None):
        self._limit = limit
        self._global_filters = global_filters

    @property
    def limit(self) -> int:
        return self._limit

    @limit.setter
    def limit(self, value: int):
        if not isinstance(value, int):
            raise ConfigurateException(detail={'limit': 'must be an int'})
        if value < 0:
            raise ConfigurateException(detail={'limit': 'must be > 0'})
        self._limit = value

    @property
    def global_filters(self) -> dict[str, Any]:
        return self._global_filters

    @global_filters.setter
    def global_filters(self, value: dict[str, Any]):
        if not isinstance(value, dict):
            raise ConfigurateException(detail={'global_filters': 'must be a dict'})
        if not all(isinstance(k, str) for k in value.keys()):
            raise ConfigurateException(detail={'global_filters': 'keys must be strings'})
        self._global_filters = value

    def configure(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


config_orm = ConfigORM()
