from enum import Enum
import os
import typing

import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

engine = sqlalchemy.create_engine(os.environ["DATABASE_URI"])
Session = sqlalchemy.orm.sessionmaker(bind=engine)


class DomainCategory(Enum):
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


class Dataset(Base):
    __tablename__ = "dataset"

    id = sqlalchemy.Column(sqlalchemy.CHAR(9), primary_key=True)

    name = sqlalchemy.Column(sqlalchemy.VARCHAR, nullable=False)
    description = sqlalchemy.Column(sqlalchemy.TEXT, nullable=False)

    owner_id = sqlalchemy.Column(
        sqlalchemy.CHAR(9), sqlalchemy.ForeignKey("owner.id"))

    domain_category = sqlalchemy.Column(
        postgresql.ENUM(
            *[v.value for v in DomainCategory.__members__.values()],
            name="DomainCategory"),
        nullable=True)

    @classmethod
    def upsert(cls, datasets: typing.Iterable["Dataset"]) -> None:
        keys = cls.__table__.c.keys()
        with engine.connect() as conn:
            for dataset in datasets:
                data = {key: getattr(dataset, key) for key in keys}
                insert = (postgresql
                          .insert(cls.__table__)
                          .values(**data)
                          .on_conflict_do_update(
                              index_elements=[cls.__table__.c.id],
                              set_={k: data[k] for k in data if k != 'id'})
                          )
                conn.execute(insert)


class Owner(Base):
    __tablename__ = "owner"
    id = sqlalchemy.Column(sqlalchemy.CHAR(9), primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.TEXT, nullable=False)

    @classmethod
    def upsert(cls, owners: typing.Iterable["Owner"]) -> None:
        keys = cls.__table__.c.keys()
        with engine.connect() as conn:
            for owner in owners:
                data = {key: getattr(owner, key) for key in keys}
                insert = (postgresql
                          .insert(cls.__table__)
                          .values(**data)
                          .on_conflict_do_update(
                              index_elements=[cls.__table__.c.id],
                              set_={k: data[k] for k in data if k != 'id'})
                          )
                conn.execute(insert)
