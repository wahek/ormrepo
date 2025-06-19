import asyncio

import pytest

from session import engine

from src.ORMrepo.models import OrmBase


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session", autouse=True)
async def setup_database():
    if not engine.url.database.endswith('test'):
        raise Exception('Use database fore test')
    async with engine.begin() as conn:
        await conn.run_sync(OrmBase.metadata.drop_all)
        await conn.run_sync(OrmBase.metadata.create_all)