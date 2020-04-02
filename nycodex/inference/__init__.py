from typing import Any, List, Sequence, Tuple

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from .. import db


def preprocess_dataset(conn: sa.engine.Connection, dataset_id: str) -> None:
    dataset = db.Session(bind=conn).query(db.Dataset).get(dataset_id)
    table = dataset.to_table(conn)

    # Use semantic information from Socrata
    allowed_type = {
        name: ty in {db.DataType.NUMBER, db.DataType.TEXT}
        for name, ty in zip(dataset.column_sql_names, dataset.column_types)
    }
    columns = [
        column
        for column in table.c
        if (
            isinstance(column.type, sa.String)
            or isinstance(column.type, sa.Integer)
        )
        if allowed_type.get(column.name, True)
    ]
    if not columns:
        return

    def length(column):
        if isinstance(column.type, sa.String):
            return sa.func.length(column)
        else:
            return column

    selects: List[Sequence[Any]] = [
        (
            sa.func.count(sa.distinct(column)).label(f"{column.name}_count"),
            sa.func.max(length(column)).label(f"{column.name}_max"),
            sa.func.min(length(column)).label(f"{column.name}_min"),
        )
        for column in columns
    ]
    selects += [
        (
            sa.func.max(column).label(f"{column.name}_max_text"),
            sa.func.min(column).label(f"{column.name}_min_text"),
        )
        for column in columns
        if isinstance(column.type, sa.String)
    ]
    selects = [item for sublist in selects for item in sublist]
    selects += [sa.func.count().label("count")]

    query = sa.select(selects).select_from(table)
    row = dict(conn.execute(query).fetchone())

    trans = conn.begin()

    for column in columns:
        data = {
            "distinct_count": row[f"{column.name}_count"],
            "unique": row[f"{column.name}_count"] == row["count"],
            "max_len": row[f"{column.name}_max"],
            "min_len": row[f"{column.name}_min"],
            "is_text": isinstance(column.type, sa.String),
            "max_text": row.get(f"{column.name}_max_text", ""),
            "min_text": row.get(f"{column.name}_min_text", ""),
        }
        if data["distinct_count"] < 4:
            # If not enough distinct counts, probably not a key
            continue
        elif column.name.startswith("boro") and data["distinct_count"] < 7:
            # Don't bother with boroughs
            # < 7 because (5 boroughs + "unknown")
            continue

        stmt = (
            postgresql.insert(db.columns)
            .values(dataset=dataset.id, column=column.name, **data)
            .on_conflict_do_update(
                index_elements=[db.columns.c.dataset, db.columns.c.column],
                set_=data,
            )
        )
        conn.execute(stmt)
    trans.commit()


def is_inclusion(
    conn: sa.engine.Connection, column: sa.Column, idataset: str, icolumn: str,
) -> bool:
    """Returns whether the `icolumn` from `idataset` includes column"""
    subquery = (
        sa.select([1])
        .select_from(db.Dataset.table(idataset))
        .where(sa.column(icolumn) == column)
    )
    iquery = (
        sa.select([1])
        .select_from(column.table)
        .where(~sa.exists(subquery))
        .limit(1)
    )
    # SELECT 1 FROM column.table WHERE NOT EXISTS
    #   SELECT 1 FROM idataset WHERE icolumn == column
    return conn.execute(iquery).fetchone() is None


def fast_filter_inclusions(
    conn: sa.engine.Connection, dataset: db.Dataset
) -> List[Tuple[str, str, str]]:
    """Find potential includers of dataset with preprocessing info

    Returns a list of (column, idataset, icolumn) tuples, where
    dataset.column is included in idataset.icolumn.
    """
    A = (
        sa.select([db.columns])
        .where(db.columns.c.dataset == dataset.id)
        .alias("A")
    )
    B = (
        sa.select([db.columns])
        .where(db.columns.c.unique & (db.columns.c.dataset != dataset.id))
        .alias("B")
    )

    query = sa.select([A.c.column, B.c.dataset, B.c.column]).where(
        sa.and_(
            A.c.is_text == B.c.is_text,
            A.c.distinct_count <= B.c.distinct_count,
            A.c.min_len >= B.c.min_len,
            A.c.min_text >= B.c.min_text,
            A.c.max_len <= B.c.max_len,
            A.c.max_text <= B.c.max_text,
        )
    )
    return conn.execute(query).fetchall()


def find_all_inclusions(conn: sa.engine.Connection, dataset_id: str) -> None:
    """Find all datasets that include a column of `dataset`"""
    dataset = db.Dataset.get_by_id(conn, dataset_id)

    table = dataset.to_table(conn)
    inclusions = [
        {
            "target_dataset": dataset.id,
            "target_column": colname,
            "source_dataset": idataset,
            "source_column": icolumn,
        }
        for colname, idataset, icolumn in fast_filter_inclusions(conn, dataset)
        if is_inclusion(conn, table.c[colname], idataset, icolumn)
    ]

    if inclusions:
        trans = conn.begin()
        conn.execute(
            db.inclusions.delete().where(
                db.inclusions.c.target_dataset == dataset.id
            )
        )
        conn.execute(db.inclusions.insert().values(inclusions))
        trans.commit()
