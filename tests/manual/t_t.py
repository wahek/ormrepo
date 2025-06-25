from typing import Any, Type, get_origin, get_args, Union, List
from pydantic import BaseModel
from sqlalchemy.orm import DeclarativeMeta

from tests.models import TModel1, RelationModel1Schema, TModel1Rel1Schema, TModel2Rel2Schema, RelationModel2Schema, \
    TModel2


class ORMBuilder:
    def __init__(self, orm_model: Type[DeclarativeMeta]):
        self.orm_model = orm_model

    def build(self, schema: BaseModel) -> DeclarativeMeta:
        return self._convert(schema, self.orm_model)

    def _convert(self, schema: BaseModel, model_class: Type[DeclarativeMeta]) -> DeclarativeMeta:
        data = {}
        for field_name, value in schema.model_dump().items():
            if isinstance(value, BaseModel):
                related_class = self._resolve_related_class(model_class, field_name)
                data[field_name] = self._convert(value, related_class)
            elif isinstance(value, list) and value and isinstance(value[0], BaseModel):
                related_class = self._resolve_related_class(model_class, field_name, many=True)
                data[field_name] = [self._convert(v, related_class) for v in value]
            else:
                data[field_name] = value

        return model_class(**data)

    def _resolve_related_class(self, model_class: Type[DeclarativeMeta], attr: str, many: bool = False) -> Type:
        annotation = model_class.__annotations__.get(attr)
        if annotation is None:
            raise ValueError(f"Attribute {attr} not found in {model_class}")

        origin = get_origin(annotation)
        args = get_args(annotation)

        if many:
            if origin in (list, List) and args:
                return args[0]
            raise TypeError(f"Expected List[Type], got {annotation}")
        else:
            return args[0] if origin is Union else annotation

schema_ = TModel1Rel1Schema(id=1, serial=1, name='1', relation_models=[RelationModel1Schema(id=1, test_model1_serial=1, test_model1_id=1)])
schema2_ = TModel2Rel2Schema(id=1, name='1', relation_model=RelationModel2Schema(id=1, test_model2_id=1))
orm_instance = ORMBuilder(TModel2).build(schema2_)