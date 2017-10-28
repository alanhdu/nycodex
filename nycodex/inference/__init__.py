import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from .. import db


def process_dataset(dataset: db.Dataset) -> None:
    with db.engine.begin() as trans:
        potential = {
            name: ty
            for name, ty in zip(dataset.column_sql_names, dataset.column_types)
            if ty in {db.DataType.NUMBER, db.DataType.TEXT}
        }
        if not potential:
            return

        selects = [sa.func.count().label("count")] + [
            sa.func.count(sa.distinct(sa.column(name))).label(f"{name}_count")
            for name in potential
        ] + [
            sa.func.max(sa.func.length(sa.column(name))).label(f"{name}_max")
            if ty == db.DataType.TEXT else
            sa.func.max(sa.column(name)).label(f"{name}_max")
            for name, ty in potential.items()
        ] + [
            sa.func.min(sa.func.length(sa.column(name))).label(f"{name}_min")
            if ty == db.DataType.TEXT else
            sa.func.min(sa.column(name)).label(f"{name}_min")
            for name, ty in potential.items()
        ]
        query = sa.select(selects).select_from(sa.table(dataset.id))
        row = trans.execute(query).fetchone()

        for name in potential:
            data = {
                "unique": row[f"{name}_count"] == row["count"],
                "max_len": row[f"{name}_max"],
                "min_len": row[f"{name}_min"],
            }
            if data['max_len'] is None:
                # Column only has NULLs
                assert data['min_len'] is None
                data['max_len'] = 0
                data['min_len'] = 0
            elif (isinstance(data['max_len'], float)
                  or isinstance(data['min_len'], float)):
                # Floating point numbers won't be foreign keys or primary keys
                continue

            stmt = postgresql.insert(db.columns) \
                .values(dataset=dataset.id, column=name, **data) \
                .on_conflict_do_update(
                    index_elements=[db.columns.c.dataset, db.columns.c.column],
                    set_=data
                )
            trans.execute(stmt)
