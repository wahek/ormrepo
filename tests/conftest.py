import asyncio
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import sessionmaker, selectinload, joinedload
from sqlalchemy.pool import NullPool

from ormrepo.exceptions import EntryNotFound
from ormrepo.orm import DatabaseRepository, DTORepository
from ormrepo.models import OrmBase
from tests.models import TModel1, TModel2, RelationModel1, RelationModel2, TModel1Schema, TModel2Schema, \
    RelationModel1Schema, RelationModel2Schema, TModel1Rel1Schema, RelRel1, TModel1Rel1RelRel1Schema
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
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            await session.close()
            raise


# --- Create/drop tables ---
@pytest.fixture(scope="session", autouse=True)
async def setup_database(engine: AsyncEngine):
    async with engine.begin() as conn:
        await conn.run_sync(OrmBase.metadata.drop_all)

        await conn.run_sync(OrmBase.metadata.create_all)


@pytest.fixture()
async def model1():
    model1_1 = TModel1(id=1, serial=1, name="m1_1")
    model1_2 = TModel1(id=2, serial=2, name="m1_2")
    model1_3 = TModel1(id=3, serial=3, name="m1_3")

    return [model1_1, model1_2, model1_3]


@pytest.fixture()
async def model2():
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


@pytest.fixture()
async def model1_related1():
    m1 = TModel1(id=4, serial=4, name="m1_4",
                 relation_models=[RelationModel1(id=4, test_model1_id=4, test_model1_serial=4)])
    m2 = TModel1(id=5, serial=5, name="m1_5",
                 relation_models=[RelationModel1(id=5, test_model1_id=5, test_model1_serial=5)])
    m3 = TModel1(id=6, serial=6, name="m1_6",
                 relation_models=[RelationModel1(id=6, test_model1_id=6, test_model1_serial=6)])
    return [m1, m2, m3]


@pytest.fixture()
async def model2_related2():
    m1 = TModel2(id=4, name="m2_4",
                 relation_model=RelationModel2(id=4, test_model2_id=4))
    m2 = TModel2(id=5, name="m2_5",
                 relation_model=RelationModel2(id=5, test_model2_id=5))
    m3 = TModel2(id=6, name="m2_6",
                 relation_model=RelationModel2(id=6, test_model2_id=6))
    return [m1, m2, m3]


@pytest.fixture()
async def model1_related1_rel_rel():
    m1 = TModel1(id=7, serial=7, name="m1_7",
                 relation_models=[RelationModel1(id=7, rel_rel1=RelRel1(id=1, ))])
    m2 = TModel1(id=8, serial=8, name="m1_8",
                 relation_models=[RelationModel1(id=8, rel_rel1=RelRel1(id=2, ))])
    m3 = TModel1(id=9, serial=9, name="m1_9",
                 relation_models=[RelationModel1(id=9, rel_rel1=RelRel1(id=3, ))])
    return [m1, m2, m3]


@pytest.fixture()
async def model1_orm(model1):
    return [TModel1Schema.model_validate(x, from_attributes=True) for x in model1]


@pytest.fixture()
async def model2_orm(model2):
    return [TModel2Schema.model_validate(x, from_attributes=True) for x in model2]


@pytest.fixture()
async def related1_orm(model1):
    return [RelationModel1Schema.model_validate(x, from_attributes=True) for x in model1]


@pytest.fixture()
async def related2_orm(model2):
    return [RelationModel2Schema.model_validate(x, from_attributes=True) for x in model2]


@pytest.fixture()
async def model1_related1_orm(model1_related1):
    return [TModel1Rel1Schema.model_validate(x, from_attributes=True) for x in model1_related1]


@pytest.fixture()
async def model1_related1_rel_rel_orm(model1_related1_rel_rel, repo_factory):
    repo = repo_factory(TModel1)
    models = await repo.get_many(
        filters=[TModel1.id.in_([x.id for x in model1_related1_rel_rel])],
        load=[
            selectinload(TModel1.relation_models).joinedload(RelationModel1.rel_rel1)
        ]
    )
    return [TModel1Rel1RelRel1Schema.model_validate(x, from_attributes=True) for x in models]


@pytest.fixture
def repo_factory(async_session):
    def _get_repo(model_cls) -> DatabaseRepository:
        return DatabaseRepository(model_cls, async_session)

    return _get_repo


@pytest.fixture
def repo_orm_factory(repo_factory):
    def _get_repo(model_cls, schema_cls) -> DTORepository:
        return DTORepository(repo_factory(model_cls), schema_cls)

    return _get_repo
