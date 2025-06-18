import logging
from typing import Generic, Any, Collection, Iterable, Literal

from sqlalchemy import ScalarResult, select, Sequence, BinaryExpression, Column, and_, ClauseElement, inspect
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import RelationshipProperty, Mapper, with_loader_criteria
from sqlalchemy.orm.strategy_options import LoaderOption

from .exceptions import ConfigurateException, ORMException, EntryNotFound
from .types_ import Model, Schema, PK
from .db_settings import config_orm

logger = logging.getLogger(__name__)


class NestedUpdater(Generic[Model]):
    """
    Service for applying nested updates to SQLAlchemy ORM instances.

    Usage:
        updated = NestedUpdater(entry).update(data)
    """

    def __init__(self, entry: Model):
        self.entry = entry

    def update(self, data: dict[str, Any]) -> Model:
        """
        Recursively apply updates from data to self.entry and return it.
        """
        self._apply(self.entry, data)
        return self.entry

    def _apply(self, entry: Any, data: dict[str, Any]) -> None:
        mapper: Mapper = inspect(entry.__class__)  # Mapper from sqlalchemy.orm.mapper
        relationships = {rel.key: rel for rel in
                         mapper.relationships}  # mapper.relationships (Expected type 'collections. Iterable', got '() -> ReadOnlyProperties[RelationshipProperty]' instead)

        for key, value in data.items():
            if key in relationships:
                rel = relationships[key]
                if rel.uselist:
                    self._update_one_to_many(entry, rel, value or [])
                else:
                    self._update_one_to_one(entry, rel, value)
            else:
                self._update_scalar(entry, key, value)

    @staticmethod
    def _update_scalar(entry: Any, key: str, value: Any) -> None:
        setattr(entry, key, value)

    def _update_one_to_one(self, entry: Any, rel: RelationshipProperty, value: Any) -> None:
        # value can be None, dict[str, Any], or ORM instance
        if value is None:
            setattr(entry, rel.key, None)
            return

        current = getattr(entry, rel.key)
        target_cls = rel.mapper.class_  # rel.mapper.class_ (Cannot find reference 'class_' in '() -> Mapper')

        if isinstance(value, dict):
            if current is None:
                new_obj = target_cls(**value)
                setattr(entry, rel.key, new_obj)
            else:
                self._apply(current, value)
        else:
            # assume ORM instance
            setattr(entry, rel.key, value)

    def _update_one_to_many(self, entry: Any, rel: RelationshipProperty, values: list[Any]) -> None:
        current_list = getattr(entry, rel.key) or []
        new_list: list[Any] = []
        target_mapper = rel.mapper
        pk_cols = target_mapper.primary_key  # target_mapper.primary_key (Cannot find reference 'primary_key' in '() -> Mapper')

        for item in values:
            if isinstance(item, dict):
                # match existing by primary key fields
                key_vals = {col.name: item[col.name] for col in pk_cols if col.name in item}
                matched = self._find_existing(current_list, key_vals)

                if matched:
                    self._apply(matched, item)
                    new_list.append(matched)
                else:
                    new_obj = target_mapper.class_(
                        **item)  # target_mapper.class_ (Cannot find reference 'class_' in '() -> Mapper' )
                    new_list.append(new_obj)
            else:
                # ORM instance
                new_list.append(item)

        setattr(entry, rel.key, new_list)

    @staticmethod
    def _find_existing(objects: list[Any], key_vals: dict[str, Any]) -> Any | None:
        for obj in objects:
            if all(getattr(obj, k) == v for k, v in key_vals.items()):
                return obj


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
    def _resolve_related_filters(related_filters: dict[type, list[ClauseElement]]
                                 ) -> list[Any]:
        options = []
        for model_class, expressions in related_filters.items():
            if expressions:
                condition = and_(*expressions)
                options.append(with_loader_criteria(model_class, condition))
        return options

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
                      load: list[LoaderOption] = None,
                      *,
                      relation_filters: dict[type[Model], list[ClauseElement]] = None,
                      ) -> Model:
        return await self._get(pk,
                               load,
                               relation_filters=relation_filters,
                               one=True)

    async def get_many(self,
                       filters: Iterable[ClauseElement] = None,
                       load: list[LoaderOption] = None,
                       *,
                       relation_filters: dict[type[Model], list[ClauseElement]] = None,
                       offset: int = None,
                       limit: int = None
                       ) -> Sequence[Model]:
        return await self._get(None,
                               filters,
                               load,
                               relation_filters=relation_filters,
                               offset=offset,
                               limit=limit)

    async def create(self,
                     model: Model) -> Model:
        self.session.add(model)
        await self.session.flush()
        return model

    async def update(self,
                     pk: PK,
                     data: dict[str, Any],
                     load: list[LoaderOption] = None,
                     *,
                     nested: bool = False
                     ) -> Model:
        entry = await self._get(pk, load=load, one=True)
        if nested:
            NestedUpdater(entry).update(data)
        else:
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
                      load: list[LoaderOption] = None
                      ) -> Schema:
        return self._schema.model_validate(await self._repo.get_one(pk, load),
                                           from_attributes=True)

    async def get_many(self,
                       filters: Iterable[ClauseElement] = None,
                       load: list[LoaderOption] = None,
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
        return self._schema.model_validate(await self._repo.update(pk, data),
                                           from_attributes=True)

    async def delete(self,
                     pk: PK) -> Schema:
        return await self._schema.model_validate(self._repo.delete(pk))


if __name__ == '__main__':
    pass
