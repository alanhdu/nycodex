import datetime as dt

import pandas as pd
import pytest
import sqlalchemy as sa

from nycodex import db, inference


def dataset_factory(id: str) -> db.Dataset:
    return db.Dataset(
        id=id,
        owner_id=id,
        description="",
        is_official=True,
        name="test1",
        page_views_total=100,
        page_views_last_week=1,
        page_views_last_month=10,
        categories=[],
        domain_category=db.DomainCategory.BUSINESS.value,
        asset_type=db.AssetType.CALENDAR.value,
        created_at=dt.datetime(2017, 1, 1, 11, 30, 0),
        updated_at=dt.datetime(2017, 1, 5, 11, 30, 0),
        is_auto_updated=True,
        parents=[],
        domain_tags=[],
        column_names=[],
        column_field_names=[],
        column_sql_names=[],
        column_types=[],
        column_descriptions=[])


@pytest.fixture
def A(conn):
    df = pd.DataFrame({
        "a": list(range(100)),
        "b": ["abcdefghijklmnopqrstuvwxyz"[i % 26] for i in range(100)]
    })  # yapf: disable
    df.to_sql("aaaa-0000", conn, index=False, schema='raw')
    return dataset_factory("aaaa-0000")


@pytest.fixture
def B(conn):
    df = pd.DataFrame({
        "a": list(range(20)) * 5,
        "b": ["abcdefghij" [i % 10] for i in range(100)]
    })
    df.to_sql("bbbb-1111", conn, index=False, schema='raw')
    return dataset_factory("bbbb-1111")


@pytest.fixture
def C(conn):
    df = pd.DataFrame({
        "a": list(range(90, 110)) * 5,
        "b": [['aa', 'bb'][i % 2] for i in range(100)]
    })
    df.to_sql("cccc-2222", conn, index=False, schema='raw')
    return dataset_factory("cccc-2222")


def test_is_inclusion(conn, A, B, C):
    A = A.to_table(conn)
    B = B.to_table(conn)
    C = C.to_table(conn)
    assert inference.is_inclusion(conn, B.c['a'], A.name, 'a')
    assert inference.is_inclusion(conn, B.c['b'], A.name, 'b')

    assert not inference.is_inclusion(conn, C.c['a'], A.name, 'a')
    assert not inference.is_inclusion(conn, C.c['b'], A.name, 'b')


def test_preprocessing(conn, session, A, B, C):
    session.add_all([A, B, C])
    session.commit()
    inference.preprocess_dataset(conn, A)
    inference.preprocess_dataset(conn, B)
    inference.preprocess_dataset(conn, C)

    assert [] == inference.fast_filter_inclusions(conn, A)
    # b is not an option because it's not unique
    assert [('a', 'aaaa-0000', 'a')] == inference.fast_filter_inclusions(
        conn, B)
    assert [] == inference.fast_filter_inclusions(conn, C)


def test_integration(conn, session, A, B, C):
    session.add_all([A, B, C])
    session.commit()
    inference.preprocess_dataset(conn, A)
    inference.preprocess_dataset(conn, B)
    inference.preprocess_dataset(conn, C)

    cquery = sa.select([sa.func.count()]).select_from(db.inclusions)
    assert 0 == conn.execute(cquery).fetchone()[0]

    inference.inclusion_dependency(conn, A)
    assert 0 == conn.execute(cquery).fetchone()[0]

    inference.inclusion_dependency(conn, C)
    assert 0 == conn.execute(cquery).fetchone()[0]

    inference.inclusion_dependency(conn, B)
    inclusions = conn.execute(sa.select([db.inclusions])).fetchall()
    assert len(inclusions) == 1
    assert dict(inclusions[0]) == {
        "source_column": "a",
        "source_dataset": A.id,
        "target_column": "a",
        "target_dataset": B.id,
    }
