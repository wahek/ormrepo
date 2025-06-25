import pytest

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload, selectinload

from ormrepo.exceptions import EntryNotFound, ORMException
from tests.conftest import repo_orm_factory
from tests.models import TModel2, TModel1, RelationModel1, RelationModel2, TModel1Schema, TModel1Rel1Schema, \
    TModel1Rel1RelRel1Schema


async def test_create_model1(repo_factory, model1):
    repo = repo_factory(TModel1)
    with pytest.raises(EntryNotFound):
        await repo.get_many()
    for model in model1:
        assert model == await repo.create(model)
    assert len(await repo.get_many()) == len(model1)


async def test_create_model2(repo_factory, model2):
    repo = repo_factory(TModel2)
    with pytest.raises(EntryNotFound):
        await repo.get_many()
    for model in model2:
        assert model == await repo.create(model)
    assert len(await repo.get_many()) == len(model2)


async def test_create_related1(repo_factory, related1):
    repo = repo_factory(RelationModel1)
    repo1 = repo_factory(TModel1)
    assert len(await repo1.get_many()) == 3
    with pytest.raises(EntryNotFound):
        await repo.get_many()
    for model in related1:
        assert model == await repo.create(model)
    assert len(await repo.get_many()) == len(related1)


async def test_create_related2(repo_factory, related2):
    repo = repo_factory(RelationModel2)
    with pytest.raises(EntryNotFound):
        await repo.get_many()
    for model in related2:
        assert model == await repo.create(model)
    assert len(await repo.get_many()) == len(related2)


async def test_create_model1_with_related(repo_factory, model1_related1):
    repo = repo_factory(TModel1)
    for model in model1_related1:
        assert model == await repo.create(model)
    assert len(await repo.get_many(
        filters=[
            TModel1.id.in_([m.id for m in model1_related1])
        ])) == len(model1_related1)


async def test_create_model2_with_related(repo_factory, model2_related2):
    repo = repo_factory(TModel2)
    for model in model2_related2:
        assert model == await repo.create(model)
    assert len(await repo.get_many(
        filters=[
            TModel2.id.in_([m.id for m in model2_related2])
        ])) == len(model2_related2)

async def test_get_filter_eq(repo_factory, model1):
    repo = repo_factory(TModel1)
    assert len(await repo.get_many(filters=[TModel1.serial == 1])) == 1

async def test_get_filter_le(repo_factory, model1):
    repo = repo_factory(TModel1)
    assert len(await repo.get_many(filters=[TModel1.serial <= 2])) == 2

async def test_get_filter_gt(repo_factory, model1):
    repo = repo_factory(TModel1)
    assert len(await repo.get_many(filters=[TModel1.serial > 4])) == 2

async def test_get_filter_like(repo_factory, model1):
    repo = repo_factory(TModel1)
    assert len(await repo.get_many(filters=[TModel1.name.like('%m1%')])) == 6
    assert len(await repo.get_many(filters=[TModel1.name.like('%_5%')])) == 1

async def test_get_filter_in(repo_factory, model1):
    repo = repo_factory(TModel1)
    assert len(await repo.get_many(filters=[TModel1.serial.in_([1, 2, 3])])) == 3


async def test_create_duplicate_model(repo_factory, model1, model2):
    repo1 = repo_factory(TModel1)
    repo2 = repo_factory(TModel2)
    for model in model1:
        with pytest.raises(IntegrityError):
            await repo1.create(model)
        await repo1.session.rollback()
    for model in model2:
        with pytest.raises(IntegrityError):
            await repo2.create(model)
        await repo2.session.rollback()


async def test_create_duplicate_relation(repo_factory, related1, related2):
    repo1 = repo_factory(RelationModel1)
    repo2 = repo_factory(RelationModel2)
    for model in related1:
        with pytest.raises(IntegrityError):
            await repo1.create(model)
        await repo1.session.rollback()
    for model in related2:
        with pytest.raises(IntegrityError):
            await repo2.create(model)
        await repo2.session.rollback()


async def test_composite_pk(repo_factory, model1):
    repo = repo_factory(TModel1)
    with pytest.raises(ORMException):
        await repo.get_one(1)
    await repo.get_one((1, 1))
    await repo.get_one([1, 1])
    await repo.get_one({'id': 1, 'serial': 1})


async def test_update_model(repo_factory, model1, model2):
    repo1 = repo_factory(TModel1)
    repo2 = repo_factory(TModel2)
    for model in model1:
        await repo1.update((model.id, model.serial), {'name': f'new_{model.name}'})
    for model in model2:
        await repo2.update(model.id, {'name': f'new_{model.name}'})


async def test_update_nested(repo_factory, model1_related1, model2_related2):
    repo1 = repo_factory(TModel1)
    repo2 = repo_factory(TModel2)
    for model in model1_related1:
        model.name = f'new_nested_{model.name}'
        model.relation_models[0].id = f'{model.id}{model.id}{model.id}'
        assert (model.__dict__ ==
                (await repo1.update((model.id, model.serial),
                                   model.__dict__,
                                   load=[selectinload(TModel1.relation_models)])).__dict__)

    for model in model2_related2:
        model.name = f'new_nested_{model.name}'
        model.relation_model.id = f'{model.id}{model.id}{model.id}'
        assert (model.__dict__ ==
                (await repo2.update(model.id,
                                    model.__dict__,
                                    load=[selectinload(TModel2.relation_model)])).__dict__)

async def test_delete_model1(repo_factory, model1):
    repo = repo_factory(TModel1)
    repo2 = repo_factory(RelationModel1)
    l = len(await repo.get_many())
    model = model1[0]
    rel = (await repo.get_one((model.id, model.serial),
                              load=[joinedload(TModel1.relation_models)])).relation_models
    assert rel is not None
    await repo.delete((model.id, model.serial))
    assert len(await repo.get_many()) == l - 1
    with pytest.raises(EntryNotFound):
        await repo.get_one((model.id, model.serial))
    for r in rel:
        with pytest.raises(EntryNotFound):
            await repo2.get_one(r.id)

async def test_orm_create_model1(repo_orm_factory, model1_orm):
    repo = repo_orm_factory(TModel1, TModel1Schema)
    l = len(await repo.get_many())
    assert model1_orm[0] == await repo.create(model1_orm[0])
    assert len(await repo.get_many()) == l + 1

async def test_orm_update_model1(repo_orm_factory, model1_orm):
    repo = repo_orm_factory(TModel1, TModel1Schema)
    model = model1_orm[0]
    model.name = f'new_{model.name}'
    print(model)
    assert model == await repo.update((model.id, model.serial), model)

async def test_orm_create_model1_related1(repo_orm_factory, model1_related1_orm):
    repo = repo_orm_factory(TModel1, TModel1Rel1Schema)

    for model in model1_related1_orm:
        await repo.delete((model.id, model.serial))
        await repo.create(model)

async def test_create_model1_related1_rel_rel(repo_factory, model1_related1_rel_rel):
    repo = repo_factory(RelationModel1)
    for model in model1_related1_rel_rel:
        await repo.create(model)


async def test_orm_update_model1_related1(repo_orm_factory, model1_related1_rel_rel_orm):
    repo = repo_orm_factory(TModel1, TModel1Rel1RelRel1Schema)
    print(model1_related1_rel_rel_orm)
    for model in model1_related1_rel_rel_orm:
        model.name = f'new_{model.name}'
        assert model == await repo.update((model.id, model.serial), model)

# async def update

