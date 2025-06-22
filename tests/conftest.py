import asyncio
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from ORMrepo.exceptions import EntryNotFound
from ORMrepo.orm import DatabaseRepository
from src.ORMrepo.models import OrmBase
from tests.models import TModel1, TModel2, RelationModel1, RelationModel2
from tests.session import uri


# --- Engine fixture ---
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def engine() -> AsyncEngine:
    return create_async_engine(uri + "_test", echo=False, poolclass=NullPool)


# --- Session fixture ---
@pytest.fixture(scope="function")
async def async_session(engine: AsyncEngine):
    async_session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    async with async_session_factory() as session:
        yield session


# --- Create/drop tables ---
@pytest.fixture(scope="function", autouse=True)
async def setup_database(engine: AsyncEngine):
    async with engine.begin() as conn:
        await conn.run_sync(OrmBase.metadata.drop_all)

        await conn.run_sync(OrmBase.metadata.create_all)


@pytest.fixture(scope='session')
async def model1():
    model1_1 = TModel1(id=1, serial=1, name="m1_1")
    model1_2 = TModel1(id=2, serial=2, name="m1_2")
    model1_3 = TModel1(id=3, serial=3, name="m1_3")

    return [model1_1, model1_2, model1_3]


@pytest.fixture()
async def model2(async_session):
    model2_1 = TModel2(id=1, name="m2_1")
    model2_2 = TModel2(id=2, name="m2_2")
    model2_3 = TModel2(id=3, name="m2_3")

    return [model2_1, model2_2, model2_3]


@pytest.fixture()
async def related1(model1: list[TModel1]):
    return [RelationModel1(id=x.id, test_model1_id=x.id, test_model1_serial=x.serial) for x in model1]


@pytest.fixture()
async def related2(model2: list[TModel2]):
    return [RelationModel2(id=x.id, test_model2_id=x.id) for x in model2]


@pytest.fixture
def repo_factory(async_session):
    def _get_repo(model_cls):
        return DatabaseRepository(model_cls, async_session)

    return _get_repo
