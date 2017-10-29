import sqlalchemy as sa

from .base import Base
from .metadata import Dataset

columns = sa.Table(
    "columns", Base.metadata,
    sa.Column("dataset", sa.TEXT, sa.ForeignKey(Dataset.__table__.c.id),
              primary_key=True),
    sa.Column("column", sa.TEXT, primary_key=True),
    sa.Column("unique", sa.BOOLEAN, nullable=False),
    sa.Column("distinct_count", sa.INTEGER, nullable=False),
    sa.Column("is_text", sa.BOOLEAN, nullable=False),

    sa.Column("max_len", sa.BIGINT, nullable=False),
    sa.Column("min_len", sa.BIGINT, nullable=False),
    sa.Column("max_text", sa.TEXT, nullable=False),
    sa.Column("min_text", sa.TEXT, nullable=False),

    schema="inference")

inclusions = sa.Table(
    "inclusions", Base.metadata,
    sa.Column("source_dataset", sa.TEXT, nullable=False),
    sa.Column("source_column", sa.TEXT, nullable=False),
    sa.Column("target_dataset", sa.TEXT, nullable=False),
    sa.Column("target_column", sa.TEXT, nullable=False),

    sa.ForeignKeyConstraint(
        ["source_dataset", "source_column"],
        ["inference.columns.dataset", "inference.columns.column"]),
    sa.ForeignKeyConstraint(
        ["target_dataset", "target_column"],
        ["inference.columns.dataset", "inference.columns.column"]),
    sa.UniqueConstraint("source_dataset", "source_column",
                        "target_dataset", "target_column",
                        name="inclusions_unique"),
    schema="inference",
)
