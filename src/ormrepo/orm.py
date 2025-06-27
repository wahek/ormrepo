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
    """
    Class for working with a database using the repository pattern.

    The repository can work with sqlalchemy models of any nesting depth via relationship.
    To perform atomic operations, create repository classes for models using a single session.

    Warnings:
        Remember, the class does not manage commit/rolling back changes to the database.
        You yourself must choose at what point to commit or roll back changes.
        Don't leave sessions open, use context manager to manage session lifecycle.
    """

    def __init__(self,
                 model: type[Model],
                 session: AsyncSession,
                 use_global_filters: bool = True):
        """
        Initializes a new instance of the repository.

        :param model: class model sqlalchemy
        :param session: Session for working with the database.
                        For atomic operations, pass one session to multiple class instances
        :param use_global_filters: Use or disable global filters for a repository instance
        """
        self.model = model
        self.session = session
        self.use_global_filters = use_global_filters

    @log()
    def _resolve_pk_condition(self, pk: PK) -> BinaryExpression:
        """Checks and apply a primary key."""
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
        """Applies or discards global filters for the model."""
        conditions = []
        for key, value in filters.items():
            if hasattr(self.model, key):
                conditions.append(getattr(self.model, key) == value)
        return and_(*conditions)

    @staticmethod
    @log()
    def _resolve_related_filters(related_filters: dict[type, list[ClauseElement]]
                                 ) -> list[Any]:
        """Applies filters to related models"""
        options = []
        for model_class, expressions in related_filters.items():
            if expressions:
                condition = and_(*expressions)
                options.append(with_loader_criteria(model_class, condition))
        return options

    @staticmethod
    @log()
    def _expand_expression(expr: BinaryExpression):
        """Expands expression for logging"""
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
        """The primary method for retrieving records from the database.
        This method is used by all other methods when accessing the database."""

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
        """
        Method to get a single record from the database

        :param pk: primary key for table (composite or regular) example: {'id': 1} | 1 | (1, 3) etc...
        :param load: sqlalchemy load options: example: [joinedload(Model.relation),...]
                     or [selectinload(Model.relation).joinedload(Model.relation.relation),...] for nested
                     models deeper than 2 levels
        :param relation_filters: filters for related models example: {Model2: [Model2.id == 1]}
        :return: sqlalchemy model
        """
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
        """
        Method to get multiple records from the database

        :param filters: filters for model example: [Model.id == 1, Model.price > 100]
                                                   or [Model.id.in_([1, 2, 3])]
                                                   or [Model.id.like_(%foo%)] etc...
        :param load: sqlalchemy load options: example: [joinedload(Model.relation),...]
                     or [selectinload(Model.relation).joinedload(Model.relation.relation),...] for nested
                     models deeper than 2 levels
        :param relation_filters: filters for related models example: {Model2: [Model2.id == 1]}
        :param offset: Number of how many records to skip
        :param limit: Maximum number of records returned
                      By default, it uses the value from ConfigORM.limit
        :return: Collection of sqlalchemy models
        """
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
        """
        Method for adding records to a session

        :param model: sqlalchemy model
        :return: added model to session
        """
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
        """
        Method for updating records in a database and adding them to a session

        :param pk: primary key for table (composite or regular) example: {'id': 1} | 1 | (1, 3) etc...
        :param data: dict
        :param load: sqlalchemy load options: example: [joinedload(Model.relation),...]
                     or [selectinload(Model.relation).joinedload(Model.relation.relation),...] for nested
                     models deeper than 2 levels
        :return: updated model in session
        """
        model = await self._get(pk, load=load, one=True)
        NestedUpdater(model).update(data)
        await self.session.flush()
        if logger.isEnabledFor(logging.INFO):
            logger.info("Added update in session %s", model)
        return model

    @log()
    async def delete(self, pk: PK) -> Model:
        """
        Method for deleting records in a database and adding them to a session

        :param pk: primary key for table (composite or regular) example: {'id': 1} | 1 | (1, 3) etc...
        :return: deleted model in session
        """
        model = await self._get(pk, one=True)
        await self.session.delete(model)
        await self.session.flush()
        if logger.isEnabledFor(logging.INFO):
            logger.info("Added deleting in session %s", model)
        return model


class DTORepository(Generic[Model, Schema]):
    """
    Class wrapper over DatabaseRepository
    """
    def __init__(self, repo: DatabaseRepository[Model],
                 schema: type[Schema]):
        """
        Initializes a new instance of the repository

        :param repo: DatabaseRepository instance
        :param schema: The scheme in which the response will be returned in all methods
                       Use a schema in which you are confident that all
                       the required fields will come from the database.
        """
        self.repo = repo
        self._schema = schema

    @log()
    def _model_validate(self, model: Model | Iterable[Model]) -> Schema | list[Schema]:
        """Method for validating models"""
        if isinstance(model, Iterable):
            return [self._schema.model_validate(x, from_attributes=True) for x in model]
        return self._schema.model_validate(model,
                                           from_attributes=True)

    @log()
    async def get_one(self,
                      pk: PK,
                      load: list[LoaderOption] = None
                      ) -> Schema:
        """
        Method to get a single record from the database and convert response from DTO.

        :param pk: primary key for table (composite or regular) example: {'id': 1} | 1 | (1, 3) etc...
        :param load: sqlalchemy load options: example: [joinedload(Model.relation),...]
                     or [selectinload(Model.relation).joinedload(Model.relation.relation),...] for nested
                     models deeper than 2 levels
        :return: pydantic schema
        """
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
        """
        Method to get multiple records from the database and convert response from DTO

        :param filters: filters for model example: [Model.id == 1, Model.price > 100]
                                                   or [Model.id.in_([1, 2, 3])]
                                                   or [Model.id.like_(%foo%)] etc...
        :param load: sqlalchemy load options: example: [joinedload(Model.relation),...]
                     or [selectinload(Model.relation).joinedload(Model.relation.relation),...] for nested
                     models deeper than 2 levels
        :param offset: Number of how many records to skip
        :param limit: Maximum number of records returned
                      By default, it uses the value from ConfigORM.limit
        :return: list of pydantic schemas
        """
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
        """
        Method for adding records to a session

        Nested models must inherit from BaseModel example:
            class Model(BaseModel):
                id: int
                name: str

            class Model2(BaseModel):
                id: int
                name: str
                model: Model

        :param schema: pydantic model
        :return: pydantic schema of the model added to the session
        """
        builder = ORMBuilder()
        model = builder.convert(schema, self.repo.model)
        res = self._model_validate(await self.repo.create(model))
        if logger.isEnabledFor(logging.INFO):
            logger.info("Serialized %d %s", 1, res)
        return res

    @log()
    async def update(self, pk: PK,
                     data: Schema,
                     load: list[LoaderOption] = None) -> Schema:
        """
        Method for updating records in a database and adding them to a session then convert response from DTO

        :param pk: primary key for table (composite or regular) example: {'id': 1} | 1 | (1, 3) etc...
        :param data: dict
        :param load: sqlalchemy load options: example: [joinedload(Model.relation),...]
                     or [selectinload(Model.relation).joinedload(Model.relation.relation),...] for nested
                     models deeper than 2 levels
        :return: pydantic schema of the updated model in session
        """
        data = data.model_dump(exclude_unset=True)
        res = self._model_validate(
            await self.repo.update(
                pk, data, load))
        if logger.isEnabledFor(logging.INFO):
            logger.info("Serialized %d %s", 1, res)
        return res

    @log()
    async def delete(self,
                     pk: PK) -> Schema:
        """
        Method for deleting records in a database and adding them to a session then convert response from DTO

        :param pk: primary key for table (composite or regular) example: {'id': 1} | 1 | (1, 3) etc...
        :return: pydantic schema of the deleted model in session
        """
        res = self._model_validate(
            await self.repo.delete(pk))
        if logger.isEnabledFor(logging.INFO):
            logger.info("Serialized %d %s", 1, res)
        return res
