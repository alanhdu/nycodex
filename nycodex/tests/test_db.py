import pytest
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
            owner_id=owner.id,
            domain_category=db.DomainCategory.RECREATION.value),
        db.Dataset(
            id="abcd-0001",
            name="y",
            description="test",
            owner_id=owner.id,
            domain_category=db.DomainCategory.ENVIRONMENT.value),
    ]

    db.Dataset.upsert(conn, datasets)
    assert session.query(db.Dataset).order_by(db.Dataset.id).all() == datasets

    # Does not insert extra columns
    db.Dataset.upsert(conn, datasets)
    assert session.query(db.Dataset).order_by(db.Dataset.id).all() == datasets

    # Handles conflicts correctly
    datasets[0].name = 'd'
    db.Dataset.upsert(conn, datasets)
    assert session.query(db.Dataset).order_by(db.Dataset.id).all() == datasets
