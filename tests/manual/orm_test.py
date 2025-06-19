import asyncio

from sqlalchemy.orm import joinedload

from src.ORMrepo.orm import DatabaseRepository, DTORepository
from tests.models import TestModel1, TestModel2, TestModel1Schema, TestModel2Schema, RelationModel1, RelationModel2
from tests.session import get_db_session


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

async def test_orm_9():
    async with get_db_session() as session:
        model = RelationModel1(test_model1_id=2, test_model1_serial=2, test_model2_id=2)
        repo1 = DatabaseRepository(RelationModel1, session)
        print(model)
        res = await repo1.create(model)
        print(res)

async def test_orm_10():
    async with get_db_session() as session:
        parent1 = TestModel1(id=3, serial=300, name="test300", relation_models=[
            RelationModel1(), RelationModel1(), RelationModel1()])

        repo1 = DatabaseRepository(TestModel1, session)
        await repo1.create(parent1)

async def test_orm_11():
    async with get_db_session() as session:
        parent2 = TestModel2(id=10, name="test10", relation_model=
        RelationModel2())
        repo2 = DatabaseRepository(TestModel2, session)
        await repo2.create(parent2)

async def test_orm_12():
    async with get_db_session() as session:
        parent2 = TestModel2(id=10)
        print(parent2)

async def test_orm_13():
    async with get_db_session() as session:
        repo = DatabaseRepository(TestModel2, session)
        res = await repo.update(1, {"name": "1test", 'relation_model': {'id': 10}},
                                load=[joinedload(TestModel2.relation_model)],
                                nested=True)
        print(res)

async def test_orm_14():
    async with get_db_session() as session:
        repo = DatabaseRepository(TestModel2, session)
        res = await repo.get_one(1, load=[joinedload(TestModel2.relation_model)],
                           relation_filters={TestModel2: [TestModel2.id == 1]})
        print(res)


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
            # test_orm_8(),
            # test_orm_9(),
            # test_orm_10(),
            # test_orm_11(),
            # test_orm_12(),
            # test_orm_13(),
            test_orm_14()
        )

    asyncio.run(main())
