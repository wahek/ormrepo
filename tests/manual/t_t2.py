from typing import Type, Any

from pydantic import BaseModel
from sqlalchemy.inspection import inspect as sqlalchemy_inspect
from sqlalchemy.util import ReadOnlyProperties

from ormrepo.types_ import Model, Schema
from tests.models import (TModel1, RelationModel1, TModel1Schema, RelationModel1Schema, TModel1Rel1Schema,
                          TModel2, RelationModel2Schema, TModel2Schema, TModel2Rel2Schema, TModel1Rel1RelRel1Schema,
                          RelationModel1RelRel1Schema, RelRelSchema)


# relationship_property = TModel1.relation_models.property
# related_model_class = relationship_property.mapper.entity
#
# print(related_model_class(id=1, test_model1_id=1, test_model1_serial=1))  # <class '__main__.Child'>

class ORMBuilder:

    def convert(self, schema: BaseModel | dict, model: Type[Any]) -> Any:
        return self._create_model_from_schema(schema, model)

    def _create_model_from_schema(self, schema: BaseModel | dict, model: Type[Any]) -> Any:
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
    def _extract_data(schema: BaseModel | dict) -> dict:
        if isinstance(schema, BaseModel):
            return schema.model_dump()
        elif isinstance(schema, dict):
            return schema
        else:
            raise TypeError(f"Unsupported schema type: {type(schema)}")

    @staticmethod
    def _is_relationship(model_cls: Type[Any], field_name: str) -> bool:
        mapper = sqlalchemy_inspect(model_cls)
        return field_name in mapper.relationships

    @staticmethod
    def _get_related_model_class(model_cls: Type[Any], field_name: str) -> Type[Any] | None:
        mapper = sqlalchemy_inspect(model_cls)
        rel = mapper.relationships.get(field_name)
        if rel is not None:
            return rel.mapper.class_
        return None


builder = ORMBuilder()
schema1 = TModel1Rel1Schema(
    id=1,
    serial=1,
    name='1',
    relation_models=[
        RelationModel1Schema(
            id=1,
            test_model1_id=1,
            test_model1_serial=1)
    ])
schema2 = TModel2Rel2Schema(
    id=1,
    name='1',
    relation_model=RelationModel2Schema(
        id=1,
        test_model2_id=1))
schema3 = TModel1Rel1RelRel1Schema(id=1,
                                   serial=1,
                                   name='1',
                                   relation_models=[
                                       RelationModel1RelRel1Schema(
                                           id=1,
                                           test_model1_id=1,
                                           test_model1_serial=1,
                                           rel_rel1=RelRelSchema(id=1, relation_model1_id=1)
                                       )
                                   ])
# print(schema3)
print(TModel1Rel1Schema.model_validate(builder.convert(schema1, TModel1), from_attributes=True))
print(TModel2Rel2Schema.model_validate(builder.convert(schema2, TModel2), from_attributes=True))
print(TModel1Rel1RelRel1Schema.model_validate(builder.convert(schema3, TModel1), from_attributes=True))
model_ = builder.convert(schema3, TModel1)
print(model_)
print(model_.relation_models)
for r in model_.relation_models:
    print(r.rel_rel1)
# print(schema1.model_dump())
