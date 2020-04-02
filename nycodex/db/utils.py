import enum
from typing import Any, Dict, Iterable, List, Type

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# TODO (SQLAlchemy 1.3). See
# https://bitbucket.org/zzzeek/sqlalchemy/issues/3906/
def enum_values(enum: Type[enum.Enum]) -> List[str]:
    return [v.value for v in enum.__members__.values()]


class DbMixin:
    __table__: sa.Table

    @classmethod
    def upsert(
        cls, conn: sa.engine.base.Connection, instances: Iterable["DbMixin"]
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

    def to_dict(self) -> Dict[str, Any]:
        keys = self.__table__.c.keys()
        return {key: getattr(self, key) for key in keys}

    def __eq__(self, other: Any) -> bool:
        return self.to_dict() == other.to_dict()
