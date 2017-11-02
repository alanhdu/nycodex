import pytest
import sqlalchemy
import testing.postgresql

from nycodex import db


class FakeTable(db.Base, db.DbMixin):
    __tablename__ = "_fake_table"

    id = sqlalchemy.Column(sqlalchemy.CHAR(9), primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.TEXT, nullable=False)
    description = sqlalchemy.Column(sqlalchemy.TEXT, nullable=True)


@pytest.fixture
def engine():
    with testing.postgresql.Postgresql() as postgresql:
        engine = sqlalchemy.create_engine(postgresql.url())
        engine.execute("CREATE SCHEMA IF NOT EXISTS inference")
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


def test_upsert(session, conn):
    fake = [
        FakeTable(id="abcd-0000", name="a", description="x"),
        FakeTable(id="abcd-0001", name="b", description="y"),
        FakeTable(id="abcd-0002", name="c", description="z"),
    ]
    FakeTable.upsert(conn, fake)
    assert session.query(FakeTable).order_by(FakeTable.id).all() == fake

    # Do not insert extra columns
    FakeTable.upsert(conn, fake)
    assert session.query(FakeTable).order_by(FakeTable.id).all() == fake

    fake[0].name = 'b'
    FakeTable.upsert(conn, fake)
    assert session.query(FakeTable).order_by(FakeTable.id).all() == fake

    # Do not overwrrite non-null columns w/ NULL
    new = [
        FakeTable(id="abcd-0002", name="d", description=None),
    ]
    FakeTable.upsert(conn, new)
    fake[2].name = 'd'
    assert session.query(FakeTable).order_by(FakeTable.id).all() == fake
