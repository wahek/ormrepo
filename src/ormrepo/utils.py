from typing import Type, Any, Generic

from pydantic import BaseModel
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Mapper, RelationshipProperty

from .logger import log
from .types_ import Model, Schema


class ORMBuilder:
    """
    Converts pydantic model to sqlalchemy model

    The class supports conversion of nested pydantic models
    into nested sqlalchemy models.
    """
    @log()
    def convert(self, schema: Schema | dict, model: Type[Model]) -> Model:
        """
        Main conversion method

        :param schema: Schema(BaseModel) | dict
        :param model: Type Model(sqlalchemy model)
        :return: sqlalchemy model
        """
        return self._create_model_from_schema(schema, model)

    def _create_model_from_schema(self, schema: Schema | dict, model: Type[Model]) -> Model:
        """Generates a model instance based on parameters from a model or dictionary"""
        data = self._extract_data(schema)
        model_kwargs = {}

        for field_name, value in data.items():
            related_class = self._get_related_model_class(model, field_name)
            if related_class:
                if isinstance(value, list):
                    model_kwargs[field_name] = [
                        self._create_model_from_schema(v, related_class) for v in value
                    ]
                elif isinstance(value, dict):
                    model_kwargs[field_name] = self._create_model_from_schema(value, related_class)
            else:
                model_kwargs[field_name] = value

        return model(**model_kwargs)

    @staticmethod
    def _extract_data(schema: Schema | dict) -> dict:
        """Extracts data from the model if it has not already been extracted."""
        if isinstance(schema, BaseModel):
            return schema.model_dump()
        elif isinstance(schema, dict):
            return schema
        else:
            raise TypeError(f"Unsupported schema type: {type(schema)}")

    @staticmethod
    def _is_relationship(model: Type[Model], field_name: str) -> bool:
        """Finds related tables in the model"""
        mapper = inspect(model)
        return field_name in mapper.relationships

    @staticmethod
    def _get_related_model_class(model: Type[Model], field_name: str) -> Type[Model] | None:
        """Gets the model class from the relationship"""
        mapper = inspect(model)
        rel = mapper.relationships.get(field_name)
        if rel is not None:
            return rel.mapper.class_
        return None


class NestedUpdater(Generic[Model]):
    """
    Updates values on a model instance

    The class supports updates of nested sqlalchemy models
     :param entry: sqlalchemy model instance
    """

    def __init__(self, entry: Model):
        self.entry = entry

    @log()
    def update(self, data: dict[str, Any]) -> Model:
        """
        Main update method

        :param data: dict
        :return: updated sqlalchemy model
        """

        self._apply(self.entry, data)
        return self.entry

    def _apply(self, entry: Model, data: dict[str, Any]) -> None:
        """Choosing which update method to choose for models or nested models"""
        mapper: Mapper = inspect(entry.__class__)
        relationships = {rel.key: rel for rel in mapper.relationships}  # type: ignore

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
    def _update_scalar(entry: Model, key: str, value: Any) -> None:
        """Updates model."""
        setattr(entry, key, value)

    def _update_one_to_one(self, entry: Model, rel: RelationshipProperty, value: Any) -> None:
        """Updates nested model. Relationship is one-to-one"""
        if value is None:
            setattr(entry, rel.key, None)
            return

        current = getattr(entry, rel.key)
        target_cls = rel.mapper.class_  # type: ignore

        if isinstance(value, dict):
            if current is None:
                new_obj = target_cls(**value)
                setattr(entry, rel.key, new_obj)
            else:
                self._apply(current, value)
        else:
            setattr(entry, rel.key, value)

    def _update_one_to_many(self, entry: Any, rel: RelationshipProperty, values: list[Any]) -> None:
        """Updates nested model. Relationship is one-to-many"""
        current_list = getattr(entry, rel.key) or []
        new_list: list[Any] = []
        target_mapper: Mapper = rel.mapper  # type: ignore
        pk_cols = target_mapper.primary_key

        for item in values:
            if isinstance(item, dict):
                key_vals = {col.name: item[col.name] for col in pk_cols if col.name in item}
                matched = self._find_existing(current_list, key_vals)

                if matched:
                    self._apply(matched, item)
                    new_list.append(matched)
                else:
                    new_obj = target_mapper.class_(
                        **item)
                    new_list.append(new_obj)
            else:
                new_list.append(item)

        setattr(entry, rel.key, new_list)

    @staticmethod
    def _find_existing(objects: list[Any], key_vals: dict[str, Any]) -> Any | None:
        for obj in objects:
            if all(getattr(obj, k) == v for k, v in key_vals.items()):
                return obj
