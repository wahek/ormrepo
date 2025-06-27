from typing import Any

from .exceptions import ConfigurateException


class ConfigORM:
    def __init__(self, limit: int = 1000, global_filters: dict[str, Any] = None):
        """
        Global config class.
        You can change the configuration at any time.

        :param limit: Limit for retrieving records from the database.
                      Used if no kwarg "limit" is passed to the repository method.
        :param global_filters: Global filter that applies to all database queries.
                               If the field is missing from the table, the parameter is discarded.
                               For example, a frequently used filter: {'is_active': True}.
                               You can always disable the application of global filters
                               when creating an instance of the repository class

        """
        self._limit = limit
        self._global_filters = global_filters

    @property
    def limit(self) -> int:
        """Limit for retrieving records from the database."""
        return self._limit

    @limit.setter
    def limit(self, value: int):
        """
        Limit for retrieving records from the database.

        :param value: int: must be > 0
        """
        if not isinstance(value, int):
            raise ConfigurateException(detail={'limit': 'must be an int'})
        if value < 0:
            raise ConfigurateException(detail={'limit': 'must be > 0'})
        self._limit = value

    @property
    def global_filters(self) -> dict[str, Any]:
        """Global filter that applies to all database queries."""
        return self._global_filters

    @global_filters.setter
    def global_filters(self, value: dict[str, Any]):
        """
        Global filter that applies to all database queries.
        If the field is missing from the table, the parameter is discarded.

        :param value: dict[str, Any]
        """
        if not isinstance(value, dict):
            raise ConfigurateException(detail={'global_filters': 'must be a dict'})
        if not all(isinstance(k, str) for k in value.keys()):
            raise ConfigurateException(detail={'global_filters': 'keys must be strings'})
        self._global_filters = value

    def configure(self, **kwargs):
        """
        Configurate ORM settings.
        :param kwargs:
        """
        for key, value in kwargs.items():
            setattr(self, key, value)


config_orm = ConfigORM()
