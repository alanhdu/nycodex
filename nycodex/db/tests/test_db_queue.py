import datetime as dt

import psycopg2
import pytest
import sqlalchemy as sa

from nycodex import db
from nycodex.db.queue import queue


def test_update_from_metadata_empty(conn):
    query = sa.select([sa.func.count()]).select_from(queue)

    assert 0 == conn.execute(query).fetchone()[0]
    db.queue.update_from_metadata(conn)
    assert 0 == conn.execute(query).fetchone()[0]


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
def fake_dataset(session):
    dataset = dataset_factory("abcd-0000")
    session.add(dataset_factory("abcd-0000"))
    session.commit()
    yield dataset.id


def test_update_from_metadata(conn, session):
    datasets = [
        dataset_factory("abcd-0000"),
        dataset_factory("abcd-0001"),
        dataset_factory("abcd-0002"),
        dataset_factory("abcd-0003"),
    ]

    datasets[0].asset_type = db.AssetType.DATASET.value
    datasets[1].asset_type = db.AssetType.MAP.value
    datasets[2].asset_type = db.AssetType.CALENDAR.value
    datasets[3].asset_type = db.AssetType.CALENDAR.value

    session.add_all(datasets)
    session.commit()

    query = sa.select([
        queue.c.dataset_id, queue.c.updated_at, queue.c.scraped_at,
        queue.c.processed_at, queue.c.retries
    ])

    orig = dt.datetime(2017, 1, 4, 11, 0, 0)
    new = dt.datetime(2017, 1, 5, 11, 30, 0)  # line up with factory

    conn.execute(queue.insert().values(
        dataset_id="abcd-0000", updated_at=orig, scraped_at=orig, retries=1))

    db.queue.update_from_metadata(conn)
    assert conn.execute(query).fetchall() == [
        ("abcd-0000", new, orig, None, 0),
        ("abcd-0001", new, None, None, 0),
    ]


def test_update_from_metadata_future(conn, session):
    dataset = dataset_factory("abcd-0000")
    dataset.asset_type = db.AssetType.DATASET.value
    dataset.updated_at = dt.datetime.now() + dt.timedelta(days=10)

    session.add(dataset)
    session.commit()

    db.queue.update_from_metadata(conn)
    assert [("abcd-0000", None, None, 0)] == conn.execute(
        sa.select([
            queue.c.dataset_id, queue.c.scraped_at, queue.c.processed_at,
            queue.c.retries
        ])).fetchall() == [("abcd-0000", None, None, 0)]

    updated_at = conn.execute(sa.select([queue.c.updated_at])).fetchone()[0]

    assert updated_at < dt.datetime.now() + dt.timedelta(minutes=1)


def test_next_row_to_scrape_empty(conn):
    with db.queue.next_row_to_scrape(conn) as (conn, dataset_id):
        assert conn is None
        assert dataset_id is None


def test_next_row_to_scrape(conn, fake_dataset):
    start = dt.datetime.now()
    orig = start - dt.timedelta(days=2)
    conn.execute(queue.insert().values(
        dataset_id=fake_dataset, retries=0, updated_at=orig))

    query = sa.select([
        queue.c.dataset_id, queue.c.updated_at, queue.c.scraped_at,
        queue.c.processed_at, queue.c.retries
    ])

    # On error, increment `retries` while leaving `updated_at` alone
    with pytest.raises(ZeroDivisionError):
        with db.queue.next_row_to_scrape(conn) as (c, dataset_id):
            assert c is not None
            assert dataset_id == "abcd-0000"
            raise ZeroDivisionError
    assert conn.execute(query).fetchone() == ("abcd-0000", orig, None, None, 1)

    # On success, reset `retries` and update `scraped_at`
    with db.queue.next_row_to_scrape(conn) as (c, dataset_id):
        assert c is not None
        assert dataset_id == fake_dataset

    row = conn.execute(query).fetchone()
    assert row.dataset_id == fake_dataset
    assert row.updated_at == orig
    assert row.processed_at is None
    assert row.retries == 0

    # Add 1 minute buffer for clock mismatch issues
    assert start - dt.timedelta(minutes=1) < row.scraped_at
    assert row.scraped_at - dt.timedelta(minutes=1) < dt.datetime.now()

    # Nothing left in the queue
    with db.queue.next_row_to_scrape(conn) as (c, dataset_id):
        assert c is None
        assert dataset_id is None


def test_next_row_to_process(conn, fake_dataset):
    start = dt.datetime.now()
    original = start - dt.timedelta(days=2)
    conn.execute(queue.insert().values(
        dataset_id=fake_dataset, retries=0, updated_at=original))

    query = sa.select([
        queue.c.dataset_id, queue.c.updated_at, queue.c.scraped_at,
        queue.c.processed_at, queue.c.retries
    ])

    # Without a scraped dataset, don't do anything
    with db.queue.next_row_to_process(conn) as (c, dataset_id):
        assert c is None
        assert dataset_id is None

    conn.execute(queue.update().where(
        queue.c.dataset_id == fake_dataset).values(scraped_at=original))

    # On success, reset `retries` and update `processed_at`
    with db.queue.next_row_to_process(conn) as (__, dataset_id):
        assert dataset_id == fake_dataset

    row = conn.execute(query).fetchone()
    assert row.dataset_id == fake_dataset
    # Add 1 minute buffer for clock mismatch issues
    assert start - dt.timedelta(minutes=1) < row.processed_at
    assert row.processed_at - dt.timedelta(minutes=1) < dt.datetime.now()
    assert row.retries == 0

    # Nothing left in the queue
    with db.queue.next_row_to_process(conn) as (c, dataset_id):
        assert c is None
        assert dataset_id is None


def test_db_failure(engine):
    Session = sa.orm.sessionmaker(bind=engine)

    with engine.connect() as conn:
        session = Session(bind=conn)
        trans = conn.begin()

        dataset = dataset_factory("abcd-0000")
        session.add(dataset)
        session.commit()

        conn.execute(queue.insert().values(
            dataset_id=dataset.id,
            retries=0,
            updated_at=dt.datetime.now() - dt.timedelta(days=1)))
        with pytest.raises(sa.exc.InternalError):
            with db.queue.next_row_to_scrape(conn) as (c, dataset_id):
                assert c is not None
                with pytest.raises(psycopg2.ProgrammingError):
                    conn.execute("SELECT * FROM non_existent_datbase")
            trans.rollback()

        assert conn.closed
