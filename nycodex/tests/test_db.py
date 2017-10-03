import datetime as dt

import pytest
import pytz
import sqlalchemy
import testing.postgresql

from nycodex import db


@pytest.fixture
def engine():
    with testing.postgresql.Postgresql() as postgresql:
        engine = sqlalchemy.create_engine(postgresql.url())
        db.Base.metadata.create_all(engine)
        yield engine


@pytest.fixture
def conn(engine):
    with engine.connect() as conn:
        yield conn


@pytest.fixture
def session(engine):
    Session = sqlalchemy.orm.sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


def test_Owner_upsert(session, conn):
    owners = [
        db.Owner(id="abcd-0000", name="a"),
        db.Owner(id="abcd-0001", name="b"),
        db.Owner(id="abcd-0002", name="c"),
    ]

    db.Owner.upsert(conn, owners)
    assert session.query(db.Owner).order_by(db.Owner.id).all() == owners

    # Does not insert extra columns
    db.Owner.upsert(conn, owners)
    assert session.query(db.Owner).order_by(db.Owner.id).all() == owners

    # Handles conflicts correctly
    owners[0].name = 'd'
    db.Owner.upsert(conn, owners)
    assert session.query(db.Owner).order_by(db.Owner.id).all() == owners


def test_Dataset_upsert(session, conn):
    owner = db.Owner(id="abcd-0000", name="owner")
    session.add(owner)
    session.commit()

    datasets = [
        db.Dataset(
            id="abcd-0000",
            name="x",
            description="test",
            is_official=True,
            updated_at=pytz.utc.localize(dt.datetime.utcnow()),
            scraped_at=pytz.utc.localize(dt.datetime.utcnow()),
            owner_id=owner.id,
            domain_category=db.DomainCategory.RECREATION,
            domain_tags=['2010', 'politics'],
            asset_type=db.AssetType.MAP),
        db.Dataset(
            id="abcd-0001",
            name="y",
            description="test",
            is_official=False,
            owner_id=owner.id,
            updated_at=pytz.utc.localize(dt.datetime.utcnow()),
            domain_category="Recreation",
            domain_tags=[],
            asset_type="map")
    ]

    db.Dataset.upsert(conn, datasets)
    assert session.query(db.Dataset).order_by(db.Dataset.id).count() == 2

    # Does not insert extra columns
    db.Dataset.upsert(conn, datasets)
    assert session.query(db.Dataset).order_by(db.Dataset.id).count() == 2

    # Handles conflicts correctly
    datasets[1].domain_category = db.DomainCategory.SOCIAL_SERVICES
    datasets[1].asset_type = db.AssetType.DATASET
    assert session.query(db.Dataset).order_by(db.Dataset.id).all() != datasets
    db.Dataset.upsert(conn, datasets)
    assert session.query(db.Dataset).order_by(db.Dataset.id).all() == datasets
