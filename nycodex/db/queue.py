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
from typing import Iterator, Optional, Tuple

import sqlalchemy as sa

from .base import Base
from .metadata import AssetType, Dataset

queue = sa.Table(
    "queue", Base.metadata,
    sa.Column("dataset_id", sa.CHAR(9), sa.ForeignKey(Dataset.__table__.c.id),
              primary_key=True),
    sa.Column("updated_at", sa.TIMESTAMP(timezone=False), nullable=False),
    sa.Column("scraped_at", sa.TIMESTAMP(timezone=False), nullable=True),
    sa.Column("processed_at", sa.TIMESTAMP(timezone=False), nullable=True),
    sa.Column("retries", sa.SMALLINT, nullable=False, default=0),
)   # yapf: disable


# TODO(alan): Use NewType for dataset_id
Pair = Tuple[Optional[sa.engine.base.Connection], Optional[str]]


def update_from_metadata(conn: sa.engine.base.Connection) -> None:
    dataset = Dataset.__table__
    query = sa.text(f"""
    INSERT INTO {queue.name}
        ({queue.c.dataset_id.name}, {queue.c.updated_at.name},
         {queue.c.retries.name})
    SELECT {dataset.c.id.name}, LEAST({dataset.c.updated_at.name}, NOW()), 0
        FROM {dataset.name}
        WHERE {dataset.c.asset_type} IN ('{AssetType.DATASET.value}',
                                         '{AssetType.MAP.value}')
          AND array_length({dataset.c.parents}, 1) IS NULL
    ON CONFLICT ({queue.c.dataset_id.name}) DO UPDATE
        SET {queue.c.updated_at.name} = excluded.{dataset.c.updated_at.name},
            {queue.c.retries.name} = 0
    """)
    conn.execute(query)


def _next_row(conn: sa.engine.base.Connection, query,
              success) -> Iterator[Pair]:
    query = (query
             .where(queue.c.retries < 3)
             .order_by(sa.asc(queue.c.retries))
             .limit(1)
             .with_for_update(skip_locked=True))  # yapf: disable

    fail = queue.update().values(retries=queue.c.retries + 1)
    trans = conn.begin()

    row = conn.execute(query).fetchone()
    if row is None:
        yield None, None
        return

    try:
        yield conn, row.dataset_id
        conn.execute(success.where(queue.c.dataset_id == row.dataset_id))
        trans.commit()
    except Exception:
        conn.execute(fail.where(queue.c.dataset_id == row.dataset_id))
        trans.commit()
        raise


@contextlib.contextmanager
def next_row_to_scrape(conn: sa.engine.Connection) -> Iterator[Pair]:
    query = (sa
             .select([queue.c.dataset_id])
             .where(sa.and_(
                 sa.or_(
                     queue.c.updated_at >= queue.c.scraped_at,
                     queue.c.scraped_at.is_(None)),
             )))  # yapf: disable
    success = queue.update().values(scraped_at=sa.func.now(), retries=0)
    yield from _next_row(conn, query, success)


@contextlib.contextmanager
def next_row_to_process(conn: sa.engine.Connection) -> Iterator[Pair]:
    query = (sa
             .select([queue.c.dataset_id])
             .where(sa.and_(
                  queue.c.scraped_at.isnot(None),
                  sa.or_(
                      queue.c.scraped_at >= queue.c.processed_at,
                      queue.c.processed_at.is_(None)),
              )))   # yapf: disable
    success = queue.update().values(processed_at=sa.func.now(), retries=0)
    yield from _next_row(conn, query, success)
