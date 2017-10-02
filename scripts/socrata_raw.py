import requests
import sqlalchemy

from nycodex import db

BASE = "https://data.cityofnewyork.us/api"


def main():
    session = db.Session()
    query = (
        session.query(db.Dataset.id, db.Dataset.asset_type)
        .filter(sqlalchemy.or_(
            db.Dataset.last_scraped.is_(None),
            db.Dataset.last_scraped < db.Dataset.updated_at))
        .filter(db.Dataset.asset_type.in_([
            db.AssetType.DATASET.value,
            db.AssetType.MAP.value,
        ]))
    )   # yapf: disable

    for dataset_id, dataset_type, in query:
        # TODO(alan): Store data somehow
        if dataset_type == db.AssetType.DATASET.value:
            params = {"accessType": "DOWNLOAD"}
            requests.get(f"{BASE}/views/{dataset_id}/rows.csv", params=params)
            # TODO(alan): Parse JSON?
        elif dataset_type == db.AssetType.MAP.value:
            # TODO(alan): Should this be Shapefile?
            params = {"method": "export", "format": "GeoJSON"}
            requests.get(f"{BASE}/geospatial/{dataset_id}", params=params)
        else:
            raise RuntimeWarning(f"Illegal dataset_type {dataset_type}")


if __name__ == "__main__":
    main()
