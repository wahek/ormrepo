import asyncio
import os
from contextlib import asynccontextmanager

from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import exc
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from src.ORMrepo.models import OrmBase
from src.tests.models import TestModel1


PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / '.env')

uri = (f"postgresql+asyncpg:"
       f"//{os.getenv('DB_USER')}"
       f":{os.getenv('DB_PASSWORD')}"
       f"@{os.getenv('DB_HOST')}"
       f":{os.getenv('DB_PORT')}"
       f"/{os.getenv('DB_NAME')}")

engine = create_async_engine(uri, echo=True)
sessionmaker = async_sessionmaker(engine)

@asynccontextmanager
async def get_db_session():

    async with sessionmaker() as session:
        try:
            yield session
            await session.commit()
        except exc.SQLAlchemyError as error:
            await session.rollback()
            raise error
        finally:
            await session.close()

async def reload_db():
    async with engine.begin() as conn:
        # await conn.run_sync(OrmBase.metadata.drop_all)
        await conn.run_sync(OrmBase.metadata.create_all)
    await engine.dispose()

if __name__ == '__main__':

    asyncio.run(reload_db())