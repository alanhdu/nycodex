"""A home-grown job-queueing system based on Postgres

To avoid having to run another system, we use a PostgreSQL table as our
queue. If our pipeline for each dataset looks like `A -> B -> C -> D`,
then we store the timestamps of the last time we ran `A`, `B`, `C`, and
`D` for each dataset. A worker for step `B` in the pipeline repeatedly:

1. `BEGIN` a transaction
2. Find a single dataset where `timestamp(A) > timestamp(B)`. For
   concurrency reasons, we take out a row-level lock via `FOR UPDATE`
   and specify `SKIP LOCKED`, so that multiple workers will find
   different datasets.
3. Do the processing step (presumably outside of PostgreSQL). Any
   updates to any PostgreSQL tables should be wrapped in the same
   transaction from 1.
4. Update `timestamp(B)` and COMMIT the transaction.

These transactions and timestamps serve as synchronization points -- if
there are dependencies between different processing steps, then they
should be split into separate processing steps to avoid stale data.

The benefits of this arrangement is that we get transactional
consistency and other ACID guarantees for free while avoiding bringing
a separate dependency like Redis.  The downside is that this is a fairly
complicated and home-grown solution and isn't very scalable.

Luckily, the number of datasets to process is small (~2500, which grows
by a couple hundred a year) and this is a batch-mode job queue, not a
low-latency online queue, so scalability shouldn't be a problem (having
a couple dozen long-lived transactions should be easy for Postgres).
"""

import contextlib

import sqlalchemy as sa

from .base import Base, engine
from .metadata import AssetType, Dataset

queue = sa.Table(
    "queue", Base.metadata,
    sa.Column("dataset_id", sa.CHAR(9), sa.ForeignKey(Dataset.__table__.c.id),
              primary_key=True),
    sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column("scraped_at", sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("processed_at", sa.TIMESTAMP(timezone=True), nullable=True),
)   # yapf: disable


def update_from_metadata(conn: sa.engine.base.Connection) -> None:
    dataset = Dataset.__table__
    conn.execute(
        sa.text(f"""
    INSERT INTO {queue.name}
    ({queue.c.dataset_id.name}, {queue.c.updated_at.name})
    SELECT {dataset.c.id.name}, {dataset.c.updated_at.name} FROM {dataset.name}
        WHERE {dataset.c.asset_type} IN
            ('{AssetType.DATASET.value}', '{AssetType.MAP.value}')
    ON CONFLICT ({queue.c.dataset_id.name}) DO UPDATE
        SET {queue.c.updated_at.name} = excluded.{dataset.c.updated_at.name}
    """))


@contextlib.contextmanager
def next_row_to_scrape():
    with engine.begin() as trans:
        query = (sa
                 .select([queue.c.dataset_id])
                 .where(sa.or_(
                      queue.c.updated_at > queue.c.scraped_at,
                      queue.c.scraped_at.is_(None)))
                 .limit(1)
                 .with_for_update(skip_locked=True))    # yapf: disable
        row = trans.execute(query).fetchone()
        if row is not None:
            yield trans, row.dataset_id
            trans.execute(
                 queue.update()
                 .values(scraped_at=sa.func.now())
                 .where(queue.c.dataset_id == row.dataset_id))  # yapf: disable
        else:
            yield trans, None


@contextlib.contextmanager
def next_row_to_process():
    with engine.begin() as trans:
        query = (sa
                 .select([queue.c.dataset_id])
                 .where(sa.and_(
                      queue.c.scraped_at.isnot(None),
                      sa.or_(
                          queue.c.scraped_at > queue.c.processed_at,
                          queue.c.processed_at.is_(None)),
                  ))
                 .limit(1)
                 .with_for_update(skip_locked=True))    # yapf: disable
        row = trans.execute(query).fetchone()
        if row is not None:
            yield trans, row.dataset_id
            trans.execute(
                 queue.update()
                 .values(processed_at=sa.func.now())
                 .where(queue.c.dataset_id == row.dataset_id))  # yapf: disable
        else:
            yield trans, None