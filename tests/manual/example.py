from pydantic import BaseModel
from sqlalchemy import Sequence
from sqlalchemy.orm import joinedload

from ormrepo.db_settings import config_orm
from ormrepo.orm import DatabaseRepository, DTORepository
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine, create_async_engine, async_sessionmaker

from tests.models import TModel1, TModel1Schema, RelationModel1, TModel1Rel1Schema, RelationModel1Schema
from tests.session import uri

config_orm.configure(limit=100, global_filters={'is_active': True})

engine: AsyncEngine = create_async_engine(uri, echo=True)
sessionmaker = async_sessionmaker(engine)


async def get_session() -> AsyncSession:
    """Session Manager"""
    async with sessionmaker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# Example of working with a repository:
async def get_models() -> Sequence[TModel1]:
    async with get_session() as session:
        repo = DatabaseRepository(TModel1,
                                  session,
                                  local_filters=[TModel1.name.like('%model%')],
                                  use_global_filters=False)
        res = await repo.get_many(filters=[TModel1.id == 1],
                                  load=[joinedload(TModel1.relation_models)],
                                  relation_filters={RelationModel1: [RelationModel1.id.in_([1, 2, 3])]},
                                  offset=10,
                                  limit=10)
        return res


# Example of working with a DTO:
async def get_dto() -> list[BaseModel]:
    async with get_session() as session:
        repo = DTORepository(DatabaseRepository(TModel1, session),
                             TModel1Schema)
        res = await repo.get_many(filters=[TModel1.id > 1])
        return res


# Example of creation
async def create_dto() -> TModel1Rel1Schema:
    async with get_session() as session:
        repo = DTORepository(DatabaseRepository(TModel1, session),
                             TModel1Rel1Schema)
        return await repo.create(TModel1Rel1Schema(name='test',
                                                   serial=1,
                                                   relation_models=[
                                                       RelationModel1Schema(),
                                                       RelationModel1Schema(),
                                                       RelationModel1Schema()
                                                   ]))


# Example of update
async def update_dto() -> TModel1Rel1Schema:
    async with get_session() as session:
        repo = DTORepository(DatabaseRepository(TModel1, session),
                             TModel1Rel1Schema)
        return await repo.update(1, TModel1Rel1Schema(name='test_new',
                                                      relation_models=None))
'''
Important:
There will be two changes to the databases here.
1: Model TModel1 name changes. Since in the update function, only those fields that are explicitly set are updated.
Because model_dump(exclude_unset=True).
2. Since we explicitly set relation_models, all records from the related table related to this record will be deleted!!!
'''