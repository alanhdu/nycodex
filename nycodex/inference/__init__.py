import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from .. import db


def process_dataset(dataset: db.Dataset) -> None:
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

    selects = [sa.func.count().label("count")] + [
        sa.func.count(sa.distinct(column)).label(f"{column.name}_count")
        for column in columns
    ] + [
        sa.func.max(length(column)).label(f"{column.name}_max")
        for column in columns
    ] + [
        sa.func.min(length(column)).label(f"{column.name}_min")
        for column in columns
    ]

    with db.engine.connect() as conn:
        query = sa.select(selects).select_from(table)
        row = conn.execute(query).fetchone()

        trans = conn.begin()

        for column in columns:
            data = {
                "unique": row[f"{column.name}_count"] == row["count"],
                "max_len": row[f"{column.name}_max"],
                "min_len": row[f"{column.name}_min"],
            }
            if data['max_len'] is None:
                # Column only has NULLs
                assert data['min_len'] is None
                data['max_len'] = 0
                data['min_len'] = 0

            stmt = postgresql.insert(db.columns) \
                .values(dataset=dataset.id, column=column.name, **data) \
                .on_conflict_do_update(
                    index_elements=[db.columns.c.dataset, db.columns.c.column],
                    set_=data
                )
            conn.execute(stmt)
        trans.commit()
