import enum
import warnings

import sqlalchemy
from sqlalchemy.dialects import postgresql

from .base import Base, engine
from .utils import DbMixin, enum_values, EnumArray


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
    CALENDAR = 'calendar'
    CHART = 'chart'
    DATALENS = 'datalens'
    DATASET = 'dataset'
    FILE = 'file'
    FILTER = 'filter'
    HREF = 'href'
    MAP = 'map'
    VISUALIZATION = 'visualization'


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


# TODO(alan): Use Array of Enums when we figure out how
class DataType:
    CALENDAR_DATE = 'calendar_date'
    CHECKBOX = 'checkbox'
    DATE = 'date'
    EMAIL = 'email'
    HTML = 'html'
    LOCATION = 'location'
    MONEY = 'money'
    NUMBER = 'number'
    PERCENT = 'percent'
    PHONE = 'phone'
    TEXT = 'text'
    URL = 'url'


class Dataset(Base, DbMixin):
    __tablename__ = "dataset"
    __table_args__ = {'schema': 'metadata'}

    id = sqlalchemy.Column(sqlalchemy.CHAR(9), primary_key=True)
    owner_id = sqlalchemy.Column(sqlalchemy.CHAR(9), nullable=False)

    attribution = sqlalchemy.Column(sqlalchemy.VARCHAR, nullable=True)
    description = sqlalchemy.Column(sqlalchemy.TEXT, nullable=False)
    is_official = sqlalchemy.Column(sqlalchemy.BOOLEAN, nullable=False)
    name = sqlalchemy.Column(sqlalchemy.VARCHAR, nullable=False)

    page_views_last_month = sqlalchemy.Column(
        sqlalchemy.INTEGER, nullable=False)
    page_views_last_week = sqlalchemy.Column(
        sqlalchemy.INTEGER, nullable=False)
    page_views_total = sqlalchemy.Column(sqlalchemy.INTEGER, nullable=False)

    created_at = sqlalchemy.Column(
        sqlalchemy.TIMESTAMP(timezone=True), nullable=False)
    updated_at = sqlalchemy.Column(
        sqlalchemy.TIMESTAMP(timezone=True), nullable=False)

    categories = sqlalchemy.Column(
        EnumArray(postgresql.ENUM(*enum_values(Category), name="Category")),
        nullable=False)
    domain_category = sqlalchemy.Column(
        postgresql.ENUM(*enum_values(DomainCategory), name="DomainCategory"),
        nullable=False)
    asset_type = sqlalchemy.Column(
        postgresql.ENUM(*enum_values(AssetType), name="AssetType"),
        nullable=False)

    dataset_agency = sqlalchemy.Column(sqlalchemy.VARCHAR, nullable=True)
    is_auto_updated = sqlalchemy.Column(sqlalchemy.BOOLEAN, nullable=False)
    update_frequency = sqlalchemy.Column(sqlalchemy.VARCHAR, nullable=True)

    parents = sqlalchemy.Column(
        sqlalchemy.ARRAY(sqlalchemy.CHAR(9)), nullable=False)
    domain_tags = sqlalchemy.Column(
        sqlalchemy.ARRAY(sqlalchemy.VARCHAR), nullable=False)

    # TODO(alan): Use Postgresql composite type
    column_names = sqlalchemy.Column(
        postgresql.ARRAY(sqlalchemy.TEXT), nullable=False)
    column_field_names = sqlalchemy.Column(
        postgresql.ARRAY(sqlalchemy.TEXT), nullable=False)
    column_sql_names = sqlalchemy.Column(
        postgresql.ARRAY(sqlalchemy.VARCHAR(63)), nullable=False)
    column_types = sqlalchemy.Column(
        postgresql.ARRAY(sqlalchemy.TEXT), nullable=False)
    column_descriptions = sqlalchemy.Column(
        postgresql.ARRAY(sqlalchemy.TEXT), nullable=False)

    __table_args__ = (
        sqlalchemy.Index(
            'idx_dataset_domain_tags_gin', domain_tags,
            postgresql_using="gin"),
        sqlalchemy.CheckConstraint("page_views_last_month >= page_views_last_week"),
        sqlalchemy.CheckConstraint("page_views_total >= page_views_last_week"),
    )

    def to_table(self) -> sqlalchemy.Table:
        with warnings.catch_warnings():
            # Ignore "did not recognize type money / geometry" warings
            warnings.simplefilter("ignore", category=sqlalchemy.exc.SAWarning)
            return sqlalchemy.Table(
                self.id, Base.metadata, autoload_with=engine)
