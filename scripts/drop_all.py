import sqlalchemy

from nycodex import db

if __name__ == "__main__":
    # Drop SQLAchemy Tables andTypes
    db.Base.metadata.drop_all(db.engine)

    # Drop scraped tables
    metadata = sqlalchemy.MetaData()
    metadata.reflect(bind=db.engine)
    with db.engine.connect() as conn:
        for key in metadata.tables:
            conn.execute(f"DROP TABLE \"{key}\" CASCADE")
