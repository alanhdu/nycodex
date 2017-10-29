import warnings

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from .. import db


def process_dataset(dataset: db.Dataset) -> None:
    with warnings.catch_warnings():
        # Ignore "did not recognize type money / geometry" warings
        warnings.simplefilter("ignore", category=sa.exc.SAWarning)
        table = sa.Table(dataset.id, db.Base.metadata, autoload_with=db.engine)
    columns = [
        column for column in table.c
        if (isinstance(column.type, sa.String)
            or isinstance(column.type, sa.Integer))
    ]

    if dataset.column_names:
        # Use semantic information from Socrata
        types = {
            name: ty
            for name, ty in zip(dataset.column_sql_names, dataset.column_types)
        }
        columns = [
            column for column in columns
            if types.get(column.name) in
            {None, db.DataType.NUMBER, db.DataType.TEXT}
        ]
    if not columns:
        return

    def length(column):
        if isinstance(column.type, sa.String):
            return sa.func.length(column)
        else:
            return column

    selects = [(
        sa.func.count(sa.distinct(column)).label(f"{column.name}_count"),
        sa.func.max(length(column)).label(f"{column.name}_max"),
        sa.func.min(length(column)).label(f"{column.name}_min"),
    ) for column in columns] + [(
        sa.func.max(column).label(f"{column.name}_max_text"),
        sa.func.min(column).label(f"{column.name}_min_text"),
    ) for column in columns if isinstance(column.type, sa.String)]
    selects = [item for sublist in selects for item in sublist]
    selects += [sa.func.count().label("count")]

    with db.engine.connect() as conn:
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

            stmt = postgresql.insert(db.columns) \
                .values(dataset=dataset.id, column=column.name, **data) \
                .on_conflict_do_update(
                    index_elements=[db.columns.c.dataset, db.columns.c.column],
                    set_=data
                )
            conn.execute(stmt)
        trans.commit()
