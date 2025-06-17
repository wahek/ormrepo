import logging
from typing import Generic, Any, Collection, Iterable, Literal

from sqlalchemy import ScalarResult, select, Sequence, BinaryExpression, Column, and_, ClauseElement
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Load

from .exceptions import ConfigurateException, ORMException, EntryNotFound
from .types_ import Model, Schema, PK, orm_rep_kwargs
from .db_settings import config_orm

logger = logging.getLogger(__name__)


class DatabaseRepository(Generic[Model]):
    def __init__(self,
                 model: type[Model],
                 session: AsyncSession,
                 use_global_filters: bool = True):
        """
        Initialize the database repository.

        Args:
            model (type[Model]): The SQLAlchemy model to use for this repository.
            session (AsyncSession | None): The SQLAlchemy async session to use for this repository.
                                    Pass one session to work with atomic operations.
        """
        self.model = model
        self.session = session
        self.use_global_filters = use_global_filters

    def _resolve_pk_condition(self, pk: PK) -> BinaryExpression:
        pk_columns = self.model.__mapper__.primary_key
        if isinstance(pk, dict):
            if set(pk.keys()) != {col.key for col in pk_columns}:
                raise ORMException(f"Invalid PK keys.",
                                   detail={'got': tuple(pk.keys()),
                                           'expected': [col.key for col in pk_columns]})
            conditions = [col == pk[col.key] for col in pk_columns]
        elif isinstance(pk, tuple):
            if len(pk_columns) != len(pk):
                raise ORMException("Composite PK tuple has wrong length.",
                                   detail={'got': len(pk),
                                           'expected': len(pk_columns)})
            conditions = [col == value for col, value in zip(pk_columns, pk)]
        else:
            if len(pk_columns) != 1:
                raise ORMException("Expected composite PK, got scalar value",
                                   detail={'got': pk,
                                           'expected': [col.key for col in pk_columns]})
            conditions = [pk_columns[0] == pk]

        return and_(*conditions)

    def _resolve_global_filters(self, filters: dict[str, Any]):
        conditions = []
        for key, value in filters.items():
            if hasattr(self.model, key):
                conditions.append(getattr(self.model, key) == value)
        return and_(*conditions)

    @staticmethod
    def _expand_expression(expr: BinaryExpression):
        left = expr.left
        right = expr.right
        operator = (expr.operator.__name__
                    if hasattr(expr.operator, "__name__")
                    else str(expr.operator))

        value = getattr(right, "value", right)

        return (getattr(left, "key", str(left)),
                operator,
                value)

    async def _get(self,
                   pk: PK = None,
                   filters: Iterable[ClauseElement] = None,
                   load: list[Load] = None,
                   *,
                   offset: int = 0,
                   limit: int = None,
                   one: bool = False,
                   ) -> Model | Sequence[Model]:
        limit = (limit
                 if limit is not None
                 else config_orm.limit)
        filters = filters or []
        load = load or []

        stmt = select(self.model)
        if pk:
            stmt = stmt.where(self._resolve_pk_condition(pk))
        for option in load:
            stmt = stmt.options(option)
        print(self.use_global_filters)
        if self.use_global_filters and config_orm.global_filters:
            stmt = stmt.where(self._resolve_global_filters(config_orm.global_filters))
        if offset:
            stmt = stmt.offset(offset)
        if limit:
            stmt = stmt.limit(limit)

        result = await self.session.execute(stmt)

        if one:
            res = result.scalars().one_or_none()
        else:
            res = result.scalars().all()
        if res:
            return res
        else:
            raise EntryNotFound(detail={'pk': pk,
                                        'filters': [self._expand_expression(x) for x in filters],
                                        'load': load} |
                                       ({'global_filters': config_orm.global_filters}))

    async def get_one(self,
                      pk: PK,
                      load: list[Load] = None
                      ) -> Model:
        return await self._get(pk,
                               load,
                               one=True)

    async def get_many(self,
                       filters: Iterable[ClauseElement] = None,
                       load: list[Load] = None,
                       *,
                       offset: int = None,
                       limit: int = None
                       ) -> Sequence[Model]:
        return await self._get(None,
                               filters,
                               load,
                               offset=offset,
                               limit=limit)

    async def create(self, model: Model) -> Model:
        self.session.add(model)
        await self.session.flush()
        return model

    async def update(self, pk: PK, data: dict[str, Any]) -> Model:
        entry = await self._get(pk)
        for key, value in data.items():
            setattr(entry, key, value)
        await self.session.flush()
        return entry

    async def delete(self, pk: PK) -> Model:
        entry = await self._get(pk)
        await self.session.delete(entry)
        await self.session.flush()
        return entry


class DTORepository(Generic[Model, Schema]):
    def __init__(self, repo: DatabaseRepository[Model],
                 schema: type[Schema]):
        self._repo = repo
        self._schema = schema

    async def get_one(self,
                      pk: PK,
                      load: list[Load] = None
                      ) -> Schema:
        return self._schema.model_validate(await self._repo.get_one(pk, load),
                                           from_attributes=True)

    async def get_many(self,
                       filters: Iterable[ClauseElement] = None,
                       load: list[Load] = None,
                       *,
                       offset: int = 0,
                       limit: int = None
                       ) -> list[Schema]:
        models = await self._repo.get_many(filters,
                                           load,
                                           offset=offset,
                                           limit=limit)
        return [self._schema.model_validate(x, from_attributes=True)
                for x in models]

    async def create(self,
                     model: Model) -> Schema:
        return self._schema.model_validate(await self._repo.create(model),
                                           from_attributes=True)

    async def update(self, pk: PK,
                     data: Schema) -> Schema:
        data = data.model_dump(exclude_unset=True)
        return self._schema.model_validate(await self._repo.update(pk, data)
                                           , from_attributes=True)

    async def delete(self,
                     pk: PK) -> Schema:
        return await self._schema.model_validate(self._repo.delete(pk))


if __name__ == '__main__':
    pass
