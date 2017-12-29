import enum
import warnings

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from .base import Base, engine
from .utils import DbMixin, enum_values


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


# TODO: Use Array of Enums when psycopg2 supports it
class Category:
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


# TODO: Use Array of Enums when psycopg2 supports it
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

    id = sa.Column(sa.CHAR(9), primary_key=True)
    owner_id = sa.Column(sa.CHAR(9), nullable=False)

    attribution = sa.Column(sa.VARCHAR, nullable=True)
    description = sa.Column(sa.TEXT, nullable=False)
    is_official = sa.Column(sa.BOOLEAN, nullable=False)
    name = sa.Column(sa.VARCHAR, nullable=False)

    page_views_last_month = sa.Column(sa.INTEGER, nullable=False)
    page_views_last_week = sa.Column(sa.INTEGER, nullable=False)
    page_views_total = sa.Column(sa.INTEGER, nullable=False)

    created_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)
    updated_at = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)

    categories = sa.Column(postgresql.ARRAY(sa.TEXT), nullable=False)
    domain_category = sa.Column(
        postgresql.ENUM(*enum_values(DomainCategory), name="DomainCategory"),
        nullable=False)
    asset_type = sa.Column(
        postgresql.ENUM(*enum_values(AssetType), name="AssetType"),
        nullable=False)

    dataset_agency = sa.Column(sa.VARCHAR, nullable=True)
    is_auto_updated = sa.Column(sa.BOOLEAN, nullable=False)
    update_frequency = sa.Column(sa.VARCHAR, nullable=True)

    parents = sa.Column(postgresql.ARRAY(sa.CHAR(9)), nullable=False)
    domain_tags = sa.Column(postgresql.ARRAY(sa.VARCHAR), nullable=False)

    # TODO(alan): Use Postgresql composite type
    column_names = sa.Column(postgresql.ARRAY(sa.TEXT), nullable=False)
    column_field_names = sa.Column(postgresql.ARRAY(sa.TEXT), nullable=False)
    column_sql_names = sa.Column(
        postgresql.ARRAY(sa.VARCHAR(63)), nullable=False)
    column_types = sa.Column(postgresql.ARRAY(sa.TEXT), nullable=False)
    column_descriptions = sa.Column(postgresql.ARRAY(sa.TEXT), nullable=False)

    __tablename__ = "dataset"
    __table_args__ = (
        sa.Index(
            'idx_dataset_domain_tags_gin', domain_tags,
            postgresql_using="gin"),
        sa.CheckConstraint("page_views_last_month >= page_views_last_week"),
        sa.CheckConstraint("page_views_total >= page_views_last_week"),
    )

    def to_table(self, conn=None) -> sa.Table:
        if conn is None:
            conn = engine
        with warnings.catch_warnings():
            # Ignore "did not recognize type money / geometry" warings
            warnings.simplefilter("ignore", category=sa.exc.SAWarning)
            return sa.Table(self.id, Base.metadata, autoload_with=conn,
                            schema="raw")
