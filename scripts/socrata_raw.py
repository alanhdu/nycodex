import sqlalchemy

from nycodex import db
from nycodex.scrape import scrape_dataset, SocrataTypeError

BASE = "https://data.cityofnewyork.us/api"


def main():
    session = db.Session()
    query = (
        session.query(
            db.Dataset.id, db.Dataset.asset_type,
            db.Dataset.column_names, db.Dataset.column_field_names,
            db.Dataset.column_types
        )
        .filter(sqlalchemy.or_(
            db.Dataset.scraped_at.is_(None),
            db.Dataset.scraped_at < db.Dataset.updated_at))
        .filter(db.Dataset.asset_type.in_([
            db.AssetType.DATASET.value,
            db.AssetType.MAP.value,
        ]))
    )   # yapf: disable

    for dataset_id, dataset_type, names, fields, types, in query:
        print("SCRAPING", dataset_id)
        if dataset_type == db.AssetType.DATASET:
            try:
                scrape_dataset(dataset_id, names, fields, types)
            except SocrataTypeError as e:
                print(f"FAILED TO IMPORT {dataset_id}: {e}")
        elif dataset_type == db.AssetType.MAP:
            # TODO(alan): PostGIS
            # params = {"method": "export", "format": "GeoJSON"}
            # requests.get(f"{BASE}/geospatial/{dataset_id}", params=params)
            pass
        else:
            raise RuntimeWarning(f"Illegal dataset_type {dataset_type}")


if __name__ == "__main__":
    main()
