import sqlalchemy as sa

from nycodex import db


class FakeTable(db.Base, db.DbMixin):
    __tablename__ = "_fake_table"

    id = sa.Column(sa.CHAR(9), primary_key=True)
    name = sa.Column(sa.TEXT, nullable=False)
    description = sa.Column(sa.TEXT, nullable=True)


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

    fake[0].name = "b"
    FakeTable.upsert(conn, fake)
    assert session.query(FakeTable).order_by(FakeTable.id).all() == fake

    # Do not overwrrite non-null columns w/ NULL
    new = [
        FakeTable(id="abcd-0002", name="d", description=None),
    ]
    FakeTable.upsert(conn, new)
    fake[2].name = "d"
    assert session.query(FakeTable).order_by(FakeTable.id).all() == fake
