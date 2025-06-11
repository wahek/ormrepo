import logging
from typing import Generic, Any, Collection

from sqlalchemy import ScalarResult, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Load

from src.ORMrepo.exceptions import ConfigurateException, ORMException
from .types_ import Model, Schema, PK
from .db_settings import config_db

logger = logging.getLogger(__name__)


class DatabaseRepository(Generic[Model]):
    def __init__(self,
                 model: type[Model],
                 session: AsyncSession | None = None,
                 # dto_model: type[Schema] | None = None,
                 # *,
                 # as_dto: bool | None = None
                 ):
        """
        Initialize the database repository.

        Args Required:
            model (type[Model]): The SQLAlchemy model to use for this repository.
            session (AsyncSession | None): The SQLAlchemy async session to use for this repository.
                                    Pass one session to work with atomic operations.
        """
        self.model = model
        self.session = session

    def _check_pk(self, pk: PK):
        pk_columns = self.model.__mapper__.primary_key
        if len(pk) != len(pk_columns):
            raise ORMException(message="Invalid primary key.",
                               status_code=500,
                               detail={"msg": "Invalid length of pk.",
                                       "solution": "Check if primary key is composite or regular.",
                                       "pk_provided": pk,
                                       "pk_expected": [x.name for x in pk_columns]})
        if isinstance(pk, Collection):
            pass


    async def _get(self,
                   pk: PK,
                   filters: dict[str, Any] = None,
                   load: list[Load] = None,
                   *,
                   offset: int = 0,
                   limit: int | None = None,
                   one: bool = False
                   ) -> Model | list[Model]:
        filters = filters or []
        load = load or []

        stmt = select(self.model)
        for option in load:
            stmt = stmt.options(option)
        for condition in filters:
            stmt = stmt.where(condition)
        if offset:
            stmt = stmt.offset(offset)
        if limit:
            stmt = stmt.limit(limit)

        result = await self.session.execute(stmt)

        if one:
            return result.scalars().one_or_none()
        else:
            return result.scalars().offset(offset).all()


class DTORepository(Generic[Model, Schema]):
    def __init__(self, base_repo: DatabaseRepository[Model], dto_model: type[DTO]):
        self._repo = base_repo
        self._dto_model = dto_model


if __name__ == '__main__':
    pass
