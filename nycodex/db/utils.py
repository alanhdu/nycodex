import enum
import re
from typing import Iterable, Type

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# Temporary, as SQLAlchemy only reads keys from enums currently; should
# hopefully be fixed in 1.3 as mentioned in the following issue:
# https://bitbucket.org/zzzeek/sqlalchemy/issues/3906/
def enum_values(enum: Type[enum.Enum]):
    return [v.value for v in enum.__members__.values()]


# http://docs.sqlalchemy.org/en/latest/dialects/postgresql.html#using-enum-with-array
class EnumArray(postgresql.ARRAY):
    def bind_expression(self, bindvalue):
        return sa.cast(bindvalue, self)

    def result_processor(self, dialect, coltype):
        super_rp = super().result_processor(dialect, coltype)

        def handle_raw_string(value):
            inner = re.match(r"^{(.*)}$", value).group(1)
            return inner.split(",")

        def process(value):
            return super_rp(handle_raw_string(value))

        return process


class DbMixin:
    __table__: sa.Table

    @classmethod
    def upsert(cls, conn: sa.engine.base.Connection,
               instances: Iterable["DbMixin"]) -> None:
        keys = cls.__table__.c.keys()
        for instance in instances:
            data = {
                key: getattr(instance, key)
                for key in keys if getattr(instance, key) is not None
            }
            insert = (postgresql.insert(cls.__table__).values(**data)
                      .on_conflict_do_update(
                          index_elements=[cls.__table__.c.id],
                          set_={k: data[k]
                                for k in data if k != 'id'}))
            conn.execute(insert)

    def to_dict(self):
        keys = self.__table__.c.keys()
        return {key: getattr(self, key) for key in keys}

    def __eq__(self, other):
        return self.to_dict() == other.to_dict()
