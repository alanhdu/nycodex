import enum
import os
import typing

import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()  # type: typing.Any

engine = sqlalchemy.create_engine(os.environ["DATABASE_URI"])
Session = sqlalchemy.orm.sessionmaker(bind=engine)


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


# TODO(alan): Use Array of Enums when we figure out how
@enum.unique
class DataType(enum.Enum):
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


class DbMixin:
    __table__: sqlalchemy.Table

    @classmethod
    def upsert(cls, conn: sqlalchemy.engine.base.Connection,
               instances: typing.Iterable["DbMixin"]) -> None:
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


def sql_enum(enum: typing.Type[enum.Enum]):
    return type(enum.__name__, (), {
        "__members__": {v.value: v for v in enum.__members__.values()}
    })  # yapf: disable


class Dataset(Base, DbMixin):
    __tablename__ = "dataset"

    id = sqlalchemy.Column(sqlalchemy.CHAR(9), primary_key=True)

    name = sqlalchemy.Column(sqlalchemy.VARCHAR, nullable=False)
    description = sqlalchemy.Column(sqlalchemy.TEXT, nullable=False)
    is_official = sqlalchemy.Column(sqlalchemy.BOOLEAN, nullable=False)

    owner_id = sqlalchemy.Column(
        sqlalchemy.CHAR(9), sqlalchemy.ForeignKey("owner.id"))

    updated_at = sqlalchemy.Column(
        sqlalchemy.TIMESTAMP(timezone=True), nullable=False)
    scraped_at = sqlalchemy.Column(
        sqlalchemy.TIMESTAMP(timezone=True), nullable=True)

    domain_category = sqlalchemy.Column(
        postgresql.ENUM(sql_enum(DomainCategory), name="DomainCategory"),
        nullable=True)
    asset_type = sqlalchemy.Column(
        postgresql.ENUM(sql_enum(AssetType), name="AssetType"), nullable=True)

    domain_tags = sqlalchemy.Column(
        sqlalchemy.ARRAY(sqlalchemy.VARCHAR), nullable=False)
    __table_args__ = (sqlalchemy.Index(
        'idx_dataset_domain_tags_gin', domain_tags, postgresql_using="gin"), )

    # TODO(alan): Use Postgresql composite type
    column_names = sqlalchemy.Column(
        postgresql.ARRAY(sqlalchemy.TEXT), nullable=False)
    column_field_names = sqlalchemy.Column(
        postgresql.ARRAY(sqlalchemy.TEXT), nullable=False)
    column_types = sqlalchemy.Column(
        postgresql.ARRAY(sqlalchemy.TEXT), nullable=False)
    column_descriptions = sqlalchemy.Column(
        postgresql.ARRAY(sqlalchemy.TEXT), nullable=False)


class Owner(Base, DbMixin):
    __tablename__ = "owner"
    id = sqlalchemy.Column(sqlalchemy.CHAR(9), primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.TEXT, nullable=False)
