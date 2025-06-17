from pydantic import BaseModel
from sqlalchemy.orm import Mapped, mapped_column

from src.ORMrepo.models import OrmBase

class TestModel1(OrmBase):
    __tablename__ = "test_model1"
    id: Mapped[int] = mapped_column(primary_key=True)
    serial: Mapped[int] = mapped_column(primary_key=True,unique=True)
    name: Mapped[str]

class TestModel2(OrmBase):
    __tablename__ = "test_model2"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

class TestModel1Schema(BaseModel):
    id: int
    serial: int
    name: str

class TestModel2Schema(BaseModel):
    id: int
    name: str