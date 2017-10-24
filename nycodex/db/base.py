import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from ..config import DATABASE_URI

Base = declarative_base()  # type: typing.Any

engine = sa.create_engine(DATABASE_URI)
Session = sa.orm.sessionmaker(bind=engine)
