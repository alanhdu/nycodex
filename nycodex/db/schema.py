import enum

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from .base import Base
from .utils import UpsertMixin


@enum.unique
class AssetType(enum.Enum):
    calendar = "calendar"
    chart = "chart"
    datalens = "datalens"
    dataset = "dataset"
    file = "file"
    filter = "filter"
    href = "href"
    map = "map"
    visualization = "visualization"


@enum.unique
class DataType(enum.Enum):
    calendar_date = "calendar_date"
    checkbox = "checkbox"
    date = "date"
    email = "email"
    html = "html"
    location = "location"
    money = "money"
    multi_line = "multi_line"
    multi_polygon = "multi_polygon"
    number = "number"
    percent = "percent"
    phone = "phone"
    point = "point"
    text = "text"
    url = "url"


class Dataset(Base, UpsertMixin):
    __tablename__ = "dataset"

    id = sa.Column(sa.CHAR(9), primary_key=True)
    asset_type = sa.Column(postgresql.ENUM(AssetType), nullable=False)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)
    description = sa.Column(sa.TEXT, nullable=False)
    is_official = sa.Column(sa.BOOLEAN, nullable=False)
    name = sa.Column(sa.VARCHAR, nullable=False)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)

    fields = sa.orm.relationship("Field", back_populates="dataset")


class Field(Base, UpsertMixin):
    __tablename__ = "field"

    dataset_id = sa.Column(
        sa.CHAR(9), sa.ForeignKey(Dataset.id), primary_key=True
    )
    field_name = sa.Column(sa.TEXT, nullable=False, primary_key=True)

    datatype = sa.Column(postgresql.ENUM(DataType), nullable=False)
    description = sa.Column(sa.TEXT, nullable=True)
    name = sa.Column(sa.TEXT, nullable=False)
    dataset = sa.orm.relationship("Dataset", back_populates="fields")


class Sketch(Base, UpsertMixin):
    __tablename__ = "sketch"
    __table_args__ = (
        sa.ForeignKeyConstraint(
            ["dataset_id", "field_name"],
            ["field.dataset_id", "field.field_name"],
        ),
    )

    dataset_id = sa.Column(
        sa.CHAR(9), sa.ForeignKey(Dataset.id), primary_key=True
    )
    field_name = sa.Column(sa.TEXT, nullable=False, primary_key=True)
    update_time = sa.Column(
        sa.TIMESTAMP(timezone=False),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )

    count = sa.Column(sa.Integer, nullable=False)
    distinct_count = sa.Column(sa.Integer, nullable=False)
