from datetime import datetime

from sqlalchemy.orm import Mapped, mapped_column

from src.ORMrepo.models import Base


class TestModel(Base):
    __tablename__ = "test_model"
    id: Mapped[int] = mapped_column(primary_key=True)
    datetime_: Mapped[datetime] = mapped_column(primary_key=True)