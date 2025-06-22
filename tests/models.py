from pydantic import BaseModel
from sqlalchemy import ForeignKey, ForeignKeyConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.ORMrepo.models import OrmBase


class TModel1(OrmBase):
    __tablename__ = "test_model1"
    id: Mapped[int] = mapped_column(primary_key=True)
    serial: Mapped[int] = mapped_column(primary_key=True, unique=True)
    name: Mapped[str]

    relation_models: Mapped[list["RelationModel1"]] = relationship(
        "RelationModel1",
        back_populates="test_model1",
        uselist=True,
        cascade="all, delete-orphan",
        primaryjoin="and_("
                    "TModel1.id == foreign(RelationModel1.test_model1_id), "
                    "TModel1.serial == foreign(RelationModel1.test_model1_serial)"
                    ")"
    )


class TModel2(OrmBase):
    __tablename__ = "test_model2"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

    relation_model: Mapped["RelationModel2"] = relationship("RelationModel2",
                                                            back_populates="test_model2",
                                                            cascade="all, delete-orphan",
                                                            uselist=False)


class TModel1Schema(BaseModel):
    id: int
    serial: int
    name: str


class TModel2Schema(BaseModel):
    id: int
    name: str


class RelationModel1(OrmBase):
    __tablename__ = "relation_model1"
    id: Mapped[int] = mapped_column(primary_key=True)


    test_model1_id: Mapped[int]
    test_model1_serial: Mapped[int]

    __table_args__ = (
        ForeignKeyConstraint(
            ("test_model1_id", "test_model1_serial"),
            ("test_model1.id", "test_model1.serial")
        ),
    )

    test_model1: Mapped["TModel1"] = relationship(
        "TModel1",
        back_populates="relation_models",
        uselist=False,
    )

class RelationModel2(OrmBase):
    __tablename__ = "relation_model2"
    id: Mapped[int] = mapped_column(primary_key=True)
    
    test_model2_id: Mapped[int] = mapped_column(ForeignKey("test_model2.id"))
    
    test_model2: Mapped["TModel2"] = relationship(
        "TModel2",
        back_populates="relation_model",
        uselist=False
    )
