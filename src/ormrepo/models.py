from sqlalchemy.orm import DeclarativeBase

class OrmBase(DeclarativeBase):
    """
    Basic model sqlalchemy for ORM
    Inherit your models from it

    You can change the parameters in its inherited model:
    repr_cols_num: count of columns output when printing a model
    repr_cols: specific columns when printing the model
    """
    repr_cols_num: int = 3
    repr_cols: tuple[str] = ()

    def __repr__(self):
        cols = []
        for idx, col in enumerate(self.__table__.columns.keys()):
            if col in self.repr_cols or idx < self.repr_cols_num:
                cols.append(f"{col}={getattr(self, col)}")

        return f"<{self.__class__.__name__} {', '.join(cols)}>"
