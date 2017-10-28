import sqlalchemy as sa

from .base import Base
from .metadata import Dataset

columns = sa.Table(
    "columns", Base.metadata,
    sa.Column("dataset", sa.TEXT, sa.ForeignKey(Dataset.__table__.c.id),
              primary_key=True),
    sa.Column("column", sa.TEXT, primary_key=True),
    sa.Column("unique", sa.BOOLEAN, nullable=False),
    sa.Column("max_len", sa.BIGINT, nullable=False),
    sa.Column("min_len", sa.BIGINT, nullable=False),
    schema="inference")
