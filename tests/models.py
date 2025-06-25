from pydantic import BaseModel
from sqlalchemy import ForeignKey, ForeignKeyConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ormrepo.models import OrmBase


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
    id: int | None = None
    serial: int | None = None
    name: str | None = None


class TModel2Schema(BaseModel):
    id: int = None
    name: str = None


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

    rel_rel1: Mapped["RelRel1"] = relationship(
        "RelRel1",
        back_populates="relation_model1",
        uselist=False)


class RelationModel2(OrmBase):
    __tablename__ = "relation_model2"
    id: Mapped[int] = mapped_column(primary_key=True)

    test_model2_id: Mapped[int] = mapped_column(ForeignKey("test_model2.id"))

    test_model2: Mapped["TModel2"] = relationship(
        "TModel2",
        back_populates="relation_model",
        uselist=False
    )


class RelRel1(OrmBase):
    __tablename__ = "rel_rel1"
    id: Mapped[int] = mapped_column(primary_key=True)

    relation_model1_id: Mapped[int] = mapped_column(ForeignKey("relation_model1.id"))

    relation_model1: Mapped["RelationModel1"] = relationship(
        "RelationModel1",
        back_populates="rel_rel1",
        uselist=False
    )


class RelationModel1Schema(BaseModel):
    id: int | None = None
    test_model1_id: int | None = None
    test_model1_serial: int | None = None


class RelationModel2Schema(BaseModel):
    id: int = None
    test_model2_id: int = None


class TModel1Rel1Schema(TModel1Schema):
    relation_models: list[RelationModel1Schema] = None


class TModel2Rel2Schema(TModel2Schema):
    relation_model: RelationModel2Schema = None

class RelRelSchema(BaseModel):
    id: int | None = None
    relation_model1_id: int | None = None

class RelationModel1RelRel1Schema(RelationModel1Schema):
    rel_rel1: RelRelSchema | None = None

class TModel1Rel1RelRel1Schema(TModel1Rel1Schema):
    relation_models: list[RelationModel1RelRel1Schema] | None = None
