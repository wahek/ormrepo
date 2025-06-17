import asyncio

from src.ORMrepo.orm import DatabaseRepository, DTORepository
from src.tests.models import TestModel1, TestModel2, TestModel1Schema, TestModel2Schema
from src.tests.session import get_db_session


async def test_orm_1():
    async with get_db_session() as session:
        repo1 = DatabaseRepository(TestModel2, session)
        print(await repo1.create(TestModel2(id=2, name='test2')))
        repo2 = DatabaseRepository(TestModel1, session)
        print(await repo2.create(TestModel1(id=2, serial=2, name='test2')))


async def test_orm_2():
    async with get_db_session() as session:
        repo1 = DatabaseRepository(TestModel2, session)
        print(await repo1.get_one(1))


async def test_orm_3():
    async with get_db_session() as session:
        repo1 = DatabaseRepository(TestModel1, session)
        print(await repo1.get_one((1, 1)))


async def test_orm_4():
    async with get_db_session() as session:
        repo1 = DatabaseRepository(TestModel1, session)
        print(await repo1.get_many(filters=[
            TestModel1.id > 30,
            TestModel1.name.like('%John%')
        ]))


async def test_orm_5():
    async with get_db_session() as session:
        repo1 = DTORepository(DatabaseRepository(TestModel1, session), TestModel1Schema)
        res = await repo1.get_one((1, 1))
        print(type(res), res)


async def test_orm_6():
    async with get_db_session() as session:
        repo1 = DTORepository(DatabaseRepository(TestModel2, session), TestModel2Schema)
        res = await repo1.get_one(1)
        print(type(res), res)


async def test_orm_7():
    async with get_db_session() as session:
        repo1 = DTORepository(DatabaseRepository(TestModel2, session), TestModel2Schema)
        res = await repo1.get_many(filters=[
            TestModel2.id == 1,
            TestModel2.name == 'test1'])
        print(type(res), res)


async def test_orm_8():
    async with get_db_session() as session:
        repo1 = DTORepository(DatabaseRepository(TestModel1, session), TestModel1Schema)
        res = await repo1.get_many(filters=[TestModel1.id == 1])
        print(type(res), res)


if __name__ == '__main__':
    async def main():
        await asyncio.gather(
            # test_orm_1(),
            # test_orm_2(),
            # test_orm_3(),
            # test_orm_4(),
            # test_orm_5(),
            # test_orm_6(),
            # test_orm_7(),
            test_orm_8()

        )

    asyncio.run(main())
