import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from .. import db


def process_dataset(dataset: db.Dataset) -> None:
    table = dataset.to_table()
    # Use semantic information from Socrata
    allowed_type = {
        name: ty in {db.DataType.NUMBER, db.DataType.TEXT}
        for name, ty in zip(dataset.column_sql_names, dataset.column_types)
    }
    columns = [
        column for column in table.c
        if (isinstance(column.type, sa.String)
            or isinstance(column.type, sa.Integer))
        if allowed_type.get(column.name, True)
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
            elif column.name.startswith("boro") and data["distinct_count"] < 7:
                # Don't bother with boroughs
                # < 7 because (5 boroughs + "unknown")
                continue

            stmt = postgresql.insert(db.columns) \
                .values(dataset=dataset.id, column=column.name, **data) \
                .on_conflict_do_update(
                    index_elements=[db.columns.c.dataset, db.columns.c.column],
                    set_=data
                )
            conn.execute(stmt)
        trans.commit()


def inclusion_dependency(dataset: db.Dataset) -> None:
    table = dataset.to_table()
    with db.engine.connect() as conn:
        A = sa.select([db.columns]) \
            .where(db.columns.c.dataset == dataset.id) \
            .alias('A')
        B = sa.select([db.columns]) \
            .where(db.columns.c.unique &
                   (db.columns.c.dataset != dataset.id)) \
            .alias('B')

        query = sa.select([A.c.column, B.c.dataset, B.c.column]) \
            .where(sa.and_(
                A.c.is_text == B.c.is_text,
                A.c.distinct_count <= B.c.distinct_count,
                A.c.min_len >= B.c.min_len,
                A.c.min_text >= B.c.min_text,
                A.c.max_len <= B.c.max_len,
                A.c.max_text <= B.c.max_text,
            ))

        inclusions = []
        for column, idataset, icolumn in conn.execute(query):
            subquery = sa.select([None]) \
                .select_from(sa.table(idataset)) \
                .where(sa.column(icolumn) == table.c[column])
            iquery = sa.select([None]) \
                .select_from(table) \
                .where(~sa.exists(subquery)) \
                .limit(1)
            if conn.execute(iquery).fetchone() is None:
                inclusions.append({
                    "target_dataset": dataset.id,
                    "target_column": column,
                    "source_dataset": idataset,
                    "source_column": icolumn
                })

        if inclusions:
            trans = conn.begin()
            conn.execute(db.inclusions.delete().where(
                db.inclusions.c.target_dataset == dataset.id))
            conn.execute(db.inclusions.insert().values(inclusions))
            trans.commit()
