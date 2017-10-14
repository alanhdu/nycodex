import sqlalchemy

from nycodex import db
from nycodex.logging import get_logger
from nycodex.scrape import scrape_dataset
from nycodex.scrape.exceptions import SocrataError

BASE = "https://data.cityofnewyork.us/api"

logger = get_logger(__name__)


def main():
    session = db.Session()
    query = (
        session.query(
            db.Dataset.id, db.Dataset.asset_type,
            db.Dataset.column_names, db.Dataset.column_sql_names,
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
        log = logger.bind(dataset_id=dataset_id)

        log.info("Scraping dataset")
        if dataset_type == db.AssetType.DATASET:
            try:
                scrape_dataset(dataset_id, names, fields, types)
            except SocrataError as e:
                log.error("Failed to import dataset", exc_info=e)
        elif dataset_type == db.AssetType.MAP:
            log.warning("Skipping map")
            # TODO(alan): PostGIS
            # params = {"method": "export", "format": "GeoJSON"}
            # requests.get(f"{BASE}/geospatial/{dataset_id}", params=params)
            pass
        else:
            raise RuntimeWarning(f"Illegal dataset_type {dataset_type}")


if __name__ == "__main__":
    main()
