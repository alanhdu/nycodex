import enum

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from .base import Base
from .utils import UpsertMixin


@enum.unique
class DomainCategory(enum.Enum):
    BIGAPPS = "NYC BigApps"
    BUSINESS = "Business"
    CITY_GOVERNMENT = "City Government"
    EDUCATION = "Education"
    ENVIRONMENT = "Environment"
    HEALTH = "Health"
    HOUSING_DEVELOPMENT = "Housing & Development"
    PUBLIC_SAFETY = "Public Safety"
    RECREATION = "Recreation"
    SOCIAL_SERVICES = "Social Services"
    TRANSPORTATION = "Transportation"


@enum.unique
class AssetType(enum.Enum):
    CALENDAR = "calendar"
    CHART = "chart"
    DATALENS = "datalens"
    DATASET = "dataset"
    FILE = "file"
    FILTER = "filter"
    HREF = "href"
    MAP = "map"
    VISUALIZATION = "visualization"


@enum.unique
class Category(enum.Enum):
    DEMOGRAPHICS = "demographics"
    ECONOMY = "economy"
    EDUCATION = "education"
    ENVIRONMENT = "environment"
    FINANCE = "finance"
    HEALTH = "health"
    HOUSING_DEVELOPMENT = "housing & development"
    INFRASTRUCTURE = "infrastructure"
    POLITICS = "politics"
    PUBLIC_SAFETY = "public safety"
    RECREATION = "recreation"
    SOCIAL_SERVICES = "social services"
    TRANSPORTATION = "transportation"


@enum.unique
class DataType(enum.Enum):
    CALENDAR_DATE = "calendar_date"
    CHECKBOX = "checkbox"
    DATE = "date"
    EMAIL = "email"
    HTML = "html"
    LOCATION = "location"
    MONEY = "money"
    NUMBER = "number"
    PERCENT = "percent"
    PHONE = "phone"
    TEXT = "text"
    URL = "url"


class Dataset(Base, UpsertMixin):
    __tablename__ = "dataset"

    id = sa.Column(sa.CHAR(9), primary_key=True)
    asset_type = sa.Column(postgresql.ENUM(AssetType), nullable=False)
    categories = sa.Column(postgresql.ARRAY(sa.TEXT), nullable=False)
    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)
    description = sa.Column(sa.TEXT, nullable=False)
    domain_category = sa.Column(
        postgresql.ENUM(DomainCategory), nullable=False
    )
    domain_tags = sa.Column(postgresql.ARRAY(sa.VARCHAR), nullable=False)
    is_official = sa.Column(sa.BOOLEAN, nullable=False)
    name = sa.Column(sa.VARCHAR, nullable=False)
    page_views_last_month = sa.Column(sa.INTEGER, nullable=False)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)

    fields = sa.orm.relationship("Field", back_populates="dataset")


class Field(Base, UpsertMixin):
    __tablename__ = "field"

    id = sa.Column(sa.Integer, primary_key=True)
    dataset_id = sa.Column(sa.CHAR(9), sa.ForeignKey(Dataset.id))
    dataset = sa.orm.relationship("Dataset", back_populates="fields")

    name = sa.Column(sa.TEXT, nullable=False)
    field_name = sa.Column(sa.TEXT, nullable=False)
    description = sa.Column(sa.TEXT, nullable=True)
    format_ = sa.Column(sa.Text, nullable=True)
    datatype = sa.Column(postgresql.ENUM(DataType), nullable=False)
