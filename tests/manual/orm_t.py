import asyncio

from sqlalchemy.orm import joinedload, selectinload

from src.ormrepo.exceptions import EntryNotFound
from src.ormrepo.orm import DatabaseRepository, DTORepository
from tests.models import TModel1, TModel2, TModel1Schema, TModel2Schema, RelationModel1, RelationModel2
from tests.session import get_db_session


async def t_orm_1():
    async with get_db_session() as session:
        repo1 = DatabaseRepository(TModel2, session)
        print(await repo1.create(TModel2(id=2, name='test2')))
        repo2 = DatabaseRepository(TModel1, session)
        print(await repo2.create(TModel1(id=2, serial=2, name='test2')))


async def t_orm_2():
    async with get_db_session() as session:
        repo1 = DatabaseRepository(TModel2, session)
        print(await repo1.get_one(1))


async def t_orm_3():
    async with get_db_session() as session:
        repo1 = DatabaseRepository(TModel1, session)
        print(await repo1.get_one((1, 1)))


async def t_orm_4():
    async with get_db_session() as session:
        repo1 = DatabaseRepository(TModel1, session)
        print(await repo1.get_many(filters=[
            TModel1.id > 30,
            TModel1.name.like('%John%')
        ]))


async def t_orm_5():
    async with get_db_session() as session:
        repo1 = DTORepository(DatabaseRepository(TModel1, session), TModel1Schema)
        res = await repo1.get_one((1, 1))
        print(type(res), res)


async def t_orm_6():
    async with get_db_session() as session:
        repo1 = DTORepository(DatabaseRepository(TModel2, session), TModel2Schema)
        res = await repo1.get_one(1)
        print(type(res), res)


async def t_orm_7():
    async with get_db_session() as session:
        repo1 = DTORepository(DatabaseRepository(TModel2, session), TModel2Schema)
        res = await repo1.get_many(filters=[
            TModel2.id == 1,
            TModel2.name == 'test1'])
        print(type(res), res)


async def t_orm_8():
    async with get_db_session() as session:
        repo1 = DTORepository(DatabaseRepository(TModel1, session), TModel1Schema)
        res = await repo1.get_many(filters=[TModel1.id == 1])
        print(type(res), res)

async def t_orm_9():
    async with get_db_session() as session:
        model = RelationModel1(test_model1_id=2, test_model1_serial=2, test_model2_id=2)
        repo1 = DatabaseRepository(RelationModel1, session)
        print(model)
        res = await repo1.create(model)
        print(res)

async def t_orm_10():
    async with get_db_session() as session:
        parent1 = TModel1(id=3, serial=300, name="test300", relation_models=[
            RelationModel1(), RelationModel1(), RelationModel1()])

        repo1 = DatabaseRepository(TModel1, session)
        await repo1.create(parent1)

async def t_orm_11():
    async with get_db_session() as session:
        parent2 = TModel2(id=10, name="test10", relation_model=
        RelationModel2())
        repo2 = DatabaseRepository(TModel2, session)
        await repo2.create(parent2)

async def t_orm_12():
    async with get_db_session() as session:
        parent2 = TModel2(id=10)
        print(parent2)

async def t_orm_13():
    async with get_db_session() as session:
        repo = DatabaseRepository(TModel2, session)
        res = await repo.update(1, {"id": 2, "name": "test1", 'relation_model': {'id': 10}},
                                load=[joinedload(TModel2.relation_model)])
        print(res)

async def t_orm_14():
    async with get_db_session() as session:
        repo = DatabaseRepository(TModel2, session)
        try:
            res = await repo.get_one(1241, load=[selectinload(TModel2.relation_model)],
                                     relation_filters={TModel2: [TModel2.id == 1]})
        except EntryNotFound as e:
            print(e.json_detail())
            # pass
        # print(res)

async def t_orm_15():
    async with get_db_session() as session:
        repo = DatabaseRepository(TModel2, session)
        try:
            res = await repo.get_many(filters=[TModel2.id > 10, TModel2.id == 1],
                                      load=[selectinload(TModel2.relation_model)])
        except EntryNotFound as e:
            print(e.json_detail())
            res = None
        return res

# async def t_orm_16():
#     async with get_db_session() as session:


if __name__ == '__main__':
    async def main():
        await asyncio.gather(
            # t_orm_1(),
            # t_orm_2(),
            # t_orm_3(),
            # t_orm_4(),
            # t_orm_5(),
            # t_orm_6(),
            # t_orm_7(),
            # t_orm_8(),
            # t_orm_9(),
            # t_orm_10(),
            # t_orm_11(),
            # t_orm_12(),
            # t_orm_13(),
            # t_orm_14(),
            t_orm_15()
        )

    asyncio.run(main())
