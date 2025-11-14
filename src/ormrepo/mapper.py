from typing import Generic, Iterable

from pydantic import BaseModel

from ormrepo.types_ import ModelT, SchemaBaseT


class Mapper(Generic[ModelT, SchemaBaseT]):
    """Interface for converting between Pydantic schemas and SQLAlchemy ORM models.

    Implementations should be pure-Python and synchronous (no DB access).
    """

    def to_model(self, schema: SchemaBaseT | dict, model_cls: type[ModelT]) -> ModelT:
        """Convert pydantic schema (or dict) into an ORM model instance.

        Must NOT add the resulting instance into a session — caller is responsible for
        `session.add()` / `session.flush()` / merging.
        """
        raise NotImplementedError

    def to_dto(self, model: ModelT | Iterable[ModelT], schema_cls: type[SchemaBaseT]) -> SchemaBaseT | list[SchemaBaseT]:
        """Convert ORM model(s) into pydantic schema(s) using `from_attributes=True`.
        Implementations should not call async DB operations.
        """
        raise NotImplementedError


class SimpleMapper(Mapper[ModelT, SchemaBaseT]):
    """Default Mapper that uses straightforward constructors and Pydantic model_validate.

    - For to_model: if `schema` is BaseModel -> call `schema.model_dump()` and pass kwargs to model_cls
      if `schema` is dict -> pass directly.
    - For to_dto: use `schema_cls.model_validate(..., from_attributes=True)`.
    """

    def to_model(self, schema: SchemaBaseT | dict, model_cls: type[ModelT]) -> ModelT:
        if isinstance(schema, BaseModel):
            data = schema.model_dump(exclude_unset=True)
        elif isinstance(schema, dict):
            data = schema
        else:
            raise TypeError("schema must be BaseModel or dict")

        # NOTE: this creates a *plain* ORM instance — not added to session.
        return model_cls(**data)

    def to_dto(self, model: ModelT | Iterable[ModelT], schema_cls: type[SchemaBaseT]) -> SchemaBaseT | list[SchemaBaseT]:
        if isinstance(model, Iterable) and not isinstance(model, (str, bytes)):
            return [schema_cls.model_validate(x, from_attributes=True) for x in model]
        return schema_cls.model_validate(model, from_attributes=True)