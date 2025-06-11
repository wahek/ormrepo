import logging

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine, AsyncSession

from src.ORMrepo.exceptions import ConfigurateException

logger = logging.getLogger(__name__)


class ConfigDB:
    def __init__(self):
        self._as_dto: bool = True
        self._engine: AsyncEngine | None = None
        self._sessionmaker: async_sessionmaker[AsyncSession] | None = None

    @property
    def as_dto(self) -> bool:
        return self._as_dto

    @as_dto.setter
    def as_dto(self, as_dto: bool):
        self._as_dto = as_dto

    @property
    def engine(self) -> AsyncEngine:
        if self._engine is None:
            raise ConfigurateException(detail={"msg": "Engine is not configured."})
        return self._engine

    @engine.setter
    def engine(self, engine: AsyncEngine):
        self._engine = engine

    @property
    def sessionmaker(self) -> async_sessionmaker[AsyncSession]:
        if self._sessionmaker is None:
            raise ConfigurateException(detail={"msg": "Sessionmaker is not configured."})
        return self._sessionmaker

    @sessionmaker.setter
    def sessionmaker(self, sessionmaker: async_sessionmaker[AsyncSession]):
        self._sessionmaker = sessionmaker

    def configure(
            self,
            *,
            engine: AsyncEngine,
            sessionmaker: async_sessionmaker,
            as_dto: bool = None,
    ):
        self.engine = engine
        self.sessionmaker = sessionmaker
        if as_dto is not None:
            self.as_dto = as_dto
        logger.debug('ConfigDB configured')


config_db = ConfigDB()

if __name__ == '__main__':
    engine_ = create_async_engine("postgresql+asyncpg://...", echo=True)
    SessionLocal = async_sessionmaker(engine_, expire_on_commit=False)
    config_db.configure(engine=engine_, sessionmaker=SessionLocal)
    print(config_db)
