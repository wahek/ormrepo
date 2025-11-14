import logging
from typing import Generic, Any, Iterable, overload

from pydantic import BaseModel
from sqlalchemy import select, Sequence, BinaryExpression, and_, ClauseElement, inspect
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import with_loader_criteria, InstrumentedAttribute
from sqlalchemy.orm.strategy_options import LoaderOption, Load
from sqlalchemy.orm.util import LoaderCriteriaOption

from .exceptions import ORMException, EntryNotFound
from .types_ import ModelT, SchemaBaseT, PK, SchemaT
from .db_settings import config_orm
from .logger import logger, format_list_log_preview, log
from .utils import NestedUpdater, ORMBuilder


class DatabaseRepository(Generic[ModelT]):
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
                 model: type[ModelT],
                 session: AsyncSession,
                 local_filters: Iterable[ClauseElement] = None,
                 local_loader_options: list[LoaderCriteriaOption] = None,
                 use_local_filters: bool = True,
                 use_local_loader_options: bool = True,
                 use_global_filters: bool = True):
        """
        Initializes a new instance of the repository.

        :param model: class model sqlalchemy
        :param session: Session for working with the database.
                        For atomic operations, pass one session to multiple class instances
        :param local_filters: Local filter that applies to all database queries for a repository instance
        :param use_global_filters: Use or disable global filters for a repository instance
        """
        self.model = model
        self.session = session
        self._local_filters = local_filters
        self._local_loader_options = local_loader_options
        self.use_local_filters = use_local_filters
        self.use_local_loader_options = use_local_loader_options
        self.use_global_filters = use_global_filters

    @property
    def local_filters(self):
        return self._local_filters

    @local_filters.setter
    def local_filters(self, value):
        if not all(isinstance(x, ClauseElement) for x in value):
            raise ORMException("local_filters must be iterable of sqlalchemy expressions",
                               detail=f'{type(value)=}')
        self._local_filters = value

    @property
    def local_loader_options(self):
        return self._local_loader_options

    @local_loader_options.setter
    def local_loader_options(self, value):
        if not all(isinstance(x, LoaderCriteriaOption) for x in value):
            raise ORMException("local_options must be iterable of sqlalchemy expressions",
                               detail=f'{type(value)=}')
        self._local_loader_options = value

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
                   join_filters: dict[type[ModelT], list[ClauseElement]] = None,
                   relation_filters: dict[type[ModelT], list[ClauseElement]] = None,
                   offset: int = 0,
                   limit: int = None,
                   one: bool = False,
                   ) -> ModelT | Sequence[ModelT]:
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

        if join_filters:
            for model, filters in join_filters.items():
                stmt = stmt.join(model)  # SQLA сам выведет ON, если FK есть

                for f in filters:
                    stmt = stmt.where(f)

        for option in load:
            stmt = stmt.options(option)

        if filters:
            stmt = stmt.where(and_(*filters))

        if self.use_local_filters and self._local_filters:
            stmt = stmt.where(and_(*self._local_filters))

        if self.use_global_filters and config_orm.global_filters:
            stmt = stmt.where(self._resolve_global_filters(config_orm.global_filters))

        if self.use_local_loader_options and self._local_loader_options:
            stmt = stmt.options(*self._local_loader_options)

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
            raise EntryNotFound(
                detail={'pk': pk,
                        'filters': [self._expand_expression(x) for x in filters],
                        'local_filters': self._local_filters if self.use_local_filters else None,
                        'global_filters': config_orm.global_filters if self.use_global_filters else None,
                        'load': load,
                        'local_loader_options': self.local_loader_options if self.use_local_loader_options else None,
                        'relation_filters': relation_filters,
                        'offset': offset if offset else None})

    @log()
    async def get_one(self,
                      pk: PK,
                      load: list[LoaderOption] = None,
                      *,
                      join_filters: dict[type[ModelT], list[ClauseElement]] = None,
                      relation_filters: dict[type[ModelT], list[ClauseElement]] = None,
                      ) -> ModelT:
        """
        Method to get a single record from the database

        :param pk: primary key for table (composite or regular) example: {'id': 1} | 1 | (1, 3) etc...
        :param load: sqlalchemy load options: example: [joinedload(Model.relation),...]
                     or [selectinload(Model.relation).joinedload(Model.relation.relation),...] for nested
                     models deeper than 2 levels
        :param join_filters: sqlalchemy filters for joined models example: {RelationModel: [RelationModel.id == 1]}
        :param relation_filters: filters for related models example: {RelationModel: [RelationModel.id == 1]}
        :return: sqlalchemy model
        """
        res = await self._get(pk,
                              load=load,
                              join_filters=join_filters,
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
                       join_filters: dict[type[ModelT], list[ClauseElement]] = None,
                       relation_filters: dict[type[ModelT], list[ClauseElement]] = None,
                       offset: int = None,
                       limit: int = None
                       ) -> Sequence[ModelT]:
        """
        Method to get multiple records from the database

        :param filters: filters for model example: [Model.id == 1, Model.price > 100]
                                                   or [Model.id.in_([1, 2, 3])]
                                                   or [Model.id.like_(%foo%)] etc...
        :param load: sqlalchemy load options: example: [joinedload(Model.relation),...]
                     or [selectinload(Model.relation).joinedload(Model.relation.relation),...] for nested
                     models deeper than 2 levels
        :param join_filters: sqlalchemy filters for joined models example: {RelationModel: [RelationModel.id == 1]}
        :param relation_filters: filters for related models example: {Model2: [Model2.id == 1]}
        :param offset: Number of how many records to skip
        :param limit: Maximum number of records returned
                      By default, it uses the value from ConfigORM.limit
        :return: Collection of sqlalchemy models
        """
        res = await self._get(None,
                              filters,
                              load,
                              join_filters=join_filters,
                              relation_filters=relation_filters,
                              offset=offset,
                              limit=limit)
        if logger.isEnabledFor(logging.INFO):
            logger.info("Received %d %s", len(res), format_list_log_preview(res))
        return res

    @log()
    async def create(self,
                     model: ModelT) -> ModelT:
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
                     load: list[LoaderOption] = None,
                     *,
                     join_filters: dict[type[ModelT], list[ClauseElement]] = None,
                     ) -> ModelT:
        """
        Method for updating records in a database and adding them to a session

        :param pk: primary key for table (composite or regular) example: {'id': 1} | 1 | (1, 3) etc...
        :param data: dict
        :param load: sqlalchemy load options: example: [joinedload(Model.relation),...]
                     or [selectinload(Model.relation).joinedload(Model.relation.relation),...] for nested
                     models deeper than 2 levels
        :param join_filters: sqlalchemy filters for joined models example: {RelationModel: [RelationModel.id == 1]}
        :return: updated model in session
        """
        model = await self._get(pk, load=load, join_filters=join_filters, one=True)
        NestedUpdater(model).update(data)
        await self.session.flush()
        if logger.isEnabledFor(logging.INFO):
            logger.info("Added update in session %s", model)
        return model

    @log()
    async def delete(self,
                     pk: PK,
                     *,
                     join_filters: dict[type[ModelT], list[ClauseElement]] = None, ) -> ModelT:
        """
        Method for deleting records in a database and adding them to a session

        :param pk: primary key for table (composite or regular) example: {'id': 1} | 1 | (1, 3) etc...
        :param join_filters: sqlalchemy filters for joined models example: {RelationModel: [RelationModel.id == 1]}

        :return: deleted model in session
        """
        model = await self._get(pk, one=True, join_filters=join_filters)
        await self.session.delete(model)
        await self.session.flush()
        if logger.isEnabledFor(logging.INFO):
            logger.info("Added deleting in session %s", model)
        return model


class DTORepository(Generic[ModelT, SchemaBaseT]):
    """
    Class wrapper over DatabaseRepository
    """

    def __init__(self,
                 repo: DatabaseRepository[ModelT],
                 schema: type[SchemaBaseT]):
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
    async def _model_validate(self,
                              model: ModelT | Iterable[ModelT],
                              refresh: bool = False,
                              schema: type[SchemaT] | None = None,
                              ) -> SchemaBaseT | list[SchemaBaseT]:
        """Method for validating models"""
        schema = schema or self._schema
        if refresh:
            await self.repo.session.refresh(model)
        if isinstance(model, Iterable):
            return [schema.model_validate(x, from_attributes=True) for x in model]
        return schema.model_validate(model,
                                     from_attributes=True)

    @overload
    async def get_one(self,
                      pk: int,
                      load: list[LoaderOption] | None = None,
                      *,
                      join_filters: dict[type[ModelT], list[ClauseElement]] = None,
                      relation_filters: dict[type[ModelT], list[ClauseElement]] = None,
                      ) -> SchemaBaseT:
        ...

    @overload
    async def get_one(self,
                      pk: int,
                      load: list[LoaderOption] | None = None,
                      *,
                      schema: type[SchemaT],
                      join_filters: dict[type[ModelT], list[ClauseElement]] = None,
                      relation_filters: dict[type[ModelT], list[ClauseElement]] = None,
                      ) -> SchemaT:
        ...

    @log()
    async def get_one(self,
                      pk: PK,
                      load: list[LoaderOption] = None,
                      *,
                      schema: type[SchemaT] | None = None,
                      join_filters: dict[type[ModelT], list[ClauseElement]] = None,
                      relation_filters: dict[type[ModelT], list[ClauseElement]] = None,
                      ) -> SchemaBaseT | SchemaT:
        """
        Method to get a single record from the database and convert response from DTO.

        :param schema: custom pydantic schema for response
        :param join_filters: sqlalchemy filters for joined models example: {RelationModel: [RelationModel.id == 1]}
        :param relation_filters: filters for related models example: {Model2: [Model2.id == 1]}
        :param pk: primary key for table (composite or regular) example: {'id': 1} | 1 | (1, 3) etc...
        :param load: sqlalchemy load options: example: [joinedload(Model.relation),...]
                     or [selectinload(Model.relation).joinedload(Model.relation.relation),...] for nested
                     models deeper than 2 levels

        :return: pydantic schema
        """
        res = await self._model_validate(
            await self.repo.get_one(pk, load,
                                    join_filters=join_filters,
                                    relation_filters=relation_filters),
            schema=schema)
        if logger.isEnabledFor(logging.INFO):
            logger.info("Serialized %d %s", 1, res)
        return res

    @overload
    async def get_many(self,
                       filters: Iterable[ClauseElement] = None,
                       load: list[LoaderOption] = None,
                       *,
                       join_filters: dict[type[ModelT], list[ClauseElement]] = None,
                       relation_filters: dict[type[ModelT], list[ClauseElement]] = None,
                       offset: int = 0,
                       limit: int = None
                       ) -> list[SchemaBaseT]:
        ...

    @overload
    async def get_many(self,
                       filters: Iterable[ClauseElement] = None,
                       load: list[LoaderOption] = None,
                       *,
                       schema: type[SchemaT],
                       join_filters: dict[type[ModelT], list[ClauseElement]] = None,
                       relation_filters: dict[type[ModelT], list[ClauseElement]] = None,
                       offset: int = 0,
                       limit: int = None
                       ) -> list[SchemaT]:
        ...

    @log()
    async def get_many(self,
                       filters: Iterable[ClauseElement] = None,
                       load: list[LoaderOption] = None,
                       *,
                       schema: type[SchemaT] | None = None,
                       join_filters: dict[type[ModelT], list[ClauseElement]] = None,
                       relation_filters: dict[type[ModelT], list[ClauseElement]] = None,
                       offset: int = 0,
                       limit: int = None
                       ) -> list[SchemaBaseT] | list[SchemaT]:
        """
        Method to get multiple records from the database and convert response from DTO

        :param schema: custom pydantic schema for response
        :param join_filters: sqlalchemy filters for joined models example: {RelationModel: [RelationModel.id == 1]}
        :param relation_filters: filters for related models example: {Model2: [Model2.id == 1]}
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
                                          join_filters=join_filters,
                                          relation_filters=relation_filters,
                                          offset=offset,
                                          limit=limit)
        res = await self._model_validate(models, schema=schema)  # type: ignore
        if logger.isEnabledFor(logging.INFO):
            logger.info("Serialized %d %s", len(res), format_list_log_preview(res))
        return res

    @overload
    async def create(self,
                     data: BaseModel,
                     ) -> SchemaBaseT:
        ...

    @overload
    async def create(self,
                     data: BaseModel,
                     *,
                     schema: type[SchemaT]
                     ) -> SchemaT:
        ...

    @log()
    async def create(self,
                     data: BaseModel,
                     *,
                     schema: type[SchemaT] | None = None,
                     ) -> SchemaBaseT:
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

        :param data: pydantic model
        :param schema: custom pydantic schema for response
        :return: pydantic schema of the model added to the session
        """
        builder = ORMBuilder()
        model = builder.convert(data, self.repo.model)
        res = await self._model_validate(await self.repo.create(model),
                                         schema)
        if logger.isEnabledFor(logging.INFO):
            logger.info("Serialized %d %s", 1, res)
        return res

    @overload
    async def update(self, pk: PK,
                     data: BaseModel,
                     load: list[LoaderOption] = None,
                     refresh: bool = False,
                     *,
                     join_filters: dict[type[ModelT], list[ClauseElement]] = None, ) -> SchemaBaseT:
        ...

    @overload
    async def update(self, pk: PK,
                     data: BaseModel,
                     load: list[LoaderOption] = None,
                     refresh: bool = False,
                     *,
                     schema: SchemaT,
                     join_filters: dict[type[ModelT], list[ClauseElement]] = None, ) -> SchemaT:
        ...


    @log()
    async def update(self, pk: PK,
                     data: BaseModel,
                     load: list[LoaderOption] = None,
                     refresh: bool = False,
                     *,
                     schema: SchemaT | None = None,
                     join_filters: dict[type[ModelT], list[ClauseElement]] = None, ) -> SchemaBaseT:
        """
        Method for updating records in a database and adding them to a session then convert response from DTO

        :param pk: primary key for table (composite or regular) example: {'id': 1} | 1 | (1, 3) etc...
        :param data: dict
        :param load: sqlalchemy load options: example: [joinedload(Model.relation),...]
                     or [selectinload(Model.relation).joinedload(Model.relation.relation),...] for nested
                     models deeper than 2 levels
        :param schema: custom pydantic schema for response
        :param refresh: Update record. Use if the model has autocomplete properties on the server side.
        :param join_filters: sqlalchemy filters for joined models example: {RelationModel: [RelationModel.id == 1]}

        :return: pydantic schema of the updated model in session
        """
        data = data.model_dump(exclude_unset=True)
        res = await self._model_validate(
            await self.repo.update(pk, data, load,
                                   join_filters=join_filters),
            refresh,
            schema=schema)
        if logger.isEnabledFor(logging.INFO):
            logger.info("Serialized %d %s", 1, res)
        return res

    @log()
    async def delete(self,
                     pk: PK,
                     join_filters: dict[type[ModelT], list[ClauseElement]] = None, ) -> SchemaBaseT:
        """
        Method for deleting records in a database and adding them to a session then convert response from DTO

        :param pk: primary key for table (composite or regular) example: {'id': 1} | 1 | (1, 3) etc...
        :param join_filters: sqlalchemy filters for joined models example: {RelationModel: [RelationModel.id == 1]}

        :return: pydantic schema of the deleted model in session
        """
        res = await self._model_validate(
            await self.repo.delete(pk, join_filters=join_filters))
        if logger.isEnabledFor(logging.INFO):
            logger.info("Serialized %d %s", 1, res)
        return res
