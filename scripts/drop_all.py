from nycodex import db

if __name__ == "__main__":
    db.Base.metadata.drop_all(db.engine)
