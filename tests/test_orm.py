import pytest

from ORMrepo.exceptions import EntryNotFound
from ORMrepo.orm import DatabaseRepository
from tests.models import TModel2, TModel1, RelationModel1, RelationModel2


async def test_create_model1(repo_factory, model1):
    repo = repo_factory(TModel1)
    with pytest.raises(EntryNotFound):
        await repo.get_many()
    for model in model1:
        assert model == await repo.create(model)
    assert len (await repo.get_many()) == len(model1)
    await repo.session.commit()

async def test_create_model2(repo_factory, model2):
    repo = repo_factory(TModel2)
    with pytest.raises(EntryNotFound):
        await repo.get_many()
    for model in model2:
        assert model == await repo.create(model)
    assert len (await repo.get_many()) == len(model2)
    await repo.session.commit()

async def test_create_related1(repo_factory, related1):
    repo = repo_factory(RelationModel1)
    repo1 = repo_factory(TModel1)
    assert len(await repo1.get_many()) == 3
    with pytest.raises(EntryNotFound):
        await repo.get_many()
    for model in related1:
        assert model == await repo.create(model)
    assert len (await repo.get_many()) == len(related1)

async def test_create_related2(repo_factory, related2):
    repo = repo_factory(RelationModel2)
    with pytest.raises(EntryNotFound):
        await repo.get_many()
    for model in related2:
        assert model == await repo.create(model)
    assert len (await repo.get_many()) == len(related2)
