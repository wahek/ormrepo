import logging
from typing import Generic, Any, Iterable

from sqlalchemy import select, Sequence, BinaryExpression, and_, ClauseElement
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import with_loader_criteria
from sqlalchemy.orm.strategy_options import LoaderOption

from .exceptions import ORMException, EntryNotFound
from .types_ import Model, Schema, PK
from .db_settings import config_orm
from .logger import logger, format_list_log_preview, log
from .utils import NestedUpdater, ORMBuilder


class DatabaseRepository(Generic[Model]):
    def __init__(self,
                 model: type[Model],
                 session: AsyncSession,
                 use_global_filters: bool = True):
        self.model = model
        self.session = session
        self.use_global_filters = use_global_filters

    @log()
    def _resolve_pk_condition(self, pk: PK) -> BinaryExpression:
        pk_columns = self.model.__mapper__.primary_key
        if isinstance(pk, dict):
            if set(pk.keys()) != {col.key for col in pk_columns}:
                raise ORMException(f"Invalid PK keys.",
                                   detail={'got': tuple(pk.keys()),
                                           'expected': [col.key for col in pk_columns]})
            conditions = [col == pk[col.key] for col in pk_columns]
        elif isinstance(pk, tuple | list):
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

    @log()
    def _resolve_global_filters(self, filters: dict[str, Any]):
        conditions = []
        for key, value in filters.items():
            if hasattr(self.model, key):
                conditions.append(getattr(self.model, key) == value)
        return and_(*conditions)

    @staticmethod
    @log()
    def _resolve_related_filters(related_filters: dict[type, list[ClauseElement]]
                                 ) -> list[Any]:
        options = []
        for model_class, expressions in related_filters.items():
            if expressions:
                condition = and_(*expressions)
                options.append(with_loader_criteria(model_class, condition))
        return options

    @staticmethod
    @log()
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

    @log()
    async def _get(self,
                   pk: PK = None,
                   filters: Iterable[ClauseElement] = None,
                   load: list[LoaderOption] = None,
                   *,
                   relation_filters: dict[Model, list[ClauseElement]] = None,
                   offset: int = 0,
                   limit: int = None,
                   one: bool = False,
                   ) -> Model | Sequence[Model]:
        limit = (limit
                 if limit is not None and limit > 0
                 else config_orm.limit)
        filters = filters or []
        load = load or []

        stmt = select(self.model)
        if pk:
            stmt = stmt.where(self._resolve_pk_condition(pk))

        for option in load:
            stmt = stmt.options(option)

        if filters:
            stmt = stmt.where(and_(*filters))

        if self.use_global_filters and config_orm.global_filters:
            stmt = stmt.where(self._resolve_global_filters(config_orm.global_filters))

        if relation_filters:
            stmt = stmt.options(*self._resolve_related_filters(relation_filters))

        if offset:
            stmt = stmt.offset(offset)
        if limit:
            stmt = stmt.limit(limit)

        result = await self.session.execute(stmt)

        if one:
            res = result.unique().scalars().one_or_none()
        else:
            res = result.unique().scalars().all()
        if res:
            return res
        else:
            raise EntryNotFound(detail={'pk': pk,
                                        'filters': [self._expand_expression(x) for x in filters],
                                        'load': load} |
                                       ({'global_filters': config_orm.global_filters}))

    @log()
    async def get_one(self,
                      pk: PK,
                      load: list[LoaderOption] = None,
                      *,
                      relation_filters: dict[type[Model], list[ClauseElement]] = None,
                      ) -> Model:
        res = await self._get(pk,
                              load=load,
                              relation_filters=relation_filters,
                              one=True)
        if logger.isEnabledFor(logging.INFO):
            logger.info("Received %d %s", 1, res)
        return res

    @log()
    async def get_many(self,
                       filters: Iterable[ClauseElement] = None,
                       load: list[LoaderOption] = None,
                       *,
                       relation_filters: dict[type[Model], list[ClauseElement]] = None,
                       offset: int = None,
                       limit: int = None
                       ) -> Sequence[Model]:
        res = await self._get(None,
                              filters,
                              load,
                              relation_filters=relation_filters,
                              offset=offset,
                              limit=limit)
        if logger.isEnabledFor(logging.INFO):
            logger.info("Received %d %s", len(res), format_list_log_preview(res))
        return res

    @log()
    async def create(self,
                     model: Model) -> Model:
        self.session.add(model)
        await self.session.flush()
        if logger.isEnabledFor(logging.INFO):
            logger.info("Added creations in session %s", model)
        return model

    async def update(self,
                     pk: PK,
                     data: dict[str, Any],
                     load: list[LoaderOption] = None
                     ) -> Model:
        model = await self._get(pk, load=load, one=True)
        NestedUpdater(model).update(data)
        await self.session.flush()
        if logger.isEnabledFor(logging.INFO):
            logger.info("Added update in session %s", model)
        return model

    @log()
    async def delete(self, pk: PK) -> Model:
        model = await self._get(pk, one=True)
        await self.session.delete(model)
        await self.session.flush()
        if logger.isEnabledFor(logging.INFO):
            logger.info("Added deleting in session %s", model)
        return model


class DTORepository(Generic[Model, Schema]):
    def __init__(self, repo: DatabaseRepository[Model],
                 schema: type[Schema]):
        self.repo = repo
        self._schema = schema

    @log()
    def _model_validate(self, model: Model | Iterable[Model]) -> Schema | list[Schema]:
        if isinstance(model, Iterable):
            return [self._schema.model_validate(x, from_attributes=True) for x in model]
        return self._schema.model_validate(model,
                                           from_attributes=True)

    @log()
    async def get_one(self,
                      pk: PK,
                      load: list[LoaderOption] = None
                      ) -> Schema:
        res = self._model_validate(
            await self.repo.get_one(pk, load))
        if logger.isEnabledFor(logging.INFO):
            logger.info("Serialized %d %s", 1, res)
        return res

    @log()
    async def get_many(self,
                       filters: Iterable[ClauseElement] = None,
                       load: list[LoaderOption] = None,
                       *,
                       offset: int = 0,
                       limit: int = None
                       ) -> list[Schema]:
        models = await self.repo.get_many(filters,
                                          load,
                                          offset=offset,
                                          limit=limit)
        res = self._model_validate(models)  # type: ignore
        if logger.isEnabledFor(logging.INFO):
            logger.info("Serialized %d %s", len(res), format_list_log_preview(res))
        return res

    @log()
    async def create(self,
                     schema: Schema) -> Schema:
        builder = ORMBuilder()
        model = builder.convert(schema, self.repo.model)
        res = self._model_validate(await self.repo.create(model))
        if logger.isEnabledFor(logging.INFO):
            logger.info("Serialized %d %s", 1, res)
        return res

    @log()
    async def update(self, pk: PK,
                     data: Schema) -> Schema:
        data = data.model_dump(exclude_unset=True)
        res = self._model_validate(
            await self.repo.update(
                pk, data))
        if logger.isEnabledFor(logging.INFO):
            logger.info("Serialized %d %s", 1, res)
        return res

    @log()
    async def delete(self,
                     pk: PK) -> Schema:
        res = self._model_validate(
            await self.repo.delete(pk))
        if logger.isEnabledFor(logging.INFO):
            logger.info("Serialized %d %s", 1, res)
        return res
