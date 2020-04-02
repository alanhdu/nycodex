from typing import Iterable

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


class UpsertMixin:
    __table__: sa.Table

    @classmethod
    def upsert(
        cls,
        conn: sa.engine.base.Connection,
        instances: Iterable["UpsertMixin"],
    ) -> None:
        keys = cls.__table__.c.keys()
        for instance in instances:
            data = {
                key: getattr(instance, key)
                for key in keys
                if getattr(instance, key) is not None
            }
            insert = (
                postgresql.insert(cls.__table__)
                .values(**data)
                .on_conflict_do_update(
                    index_elements=[cls.__table__.c.id],
                    set_={k: data[k] for k in data if k != "id"},
                )
            )
            conn.execute(insert)
