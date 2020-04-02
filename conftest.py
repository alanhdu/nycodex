import pytest
import sqlalchemy as sa
import testing.postgresql

from nycodex import db


@pytest.fixture(scope='module')
def engine():
    with testing.postgresql.Postgresql() as postgresql:
        engine = sa.create_engine(postgresql.url())
        engine.execute("CREATE SCHEMA IF NOT EXISTS inference;")
        engine.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        db.Base.metadata.create_all(engine)
        yield engine


@pytest.fixture
def conn(engine):
    with engine.connect() as conn:
        trans = conn.begin()
        yield conn
        trans.rollback()


@pytest.fixture
def session(conn):
    Session = sa.orm.sessionmaker(bind=engine)
    session = Session(bind=conn)
    yield session
    session.close()
