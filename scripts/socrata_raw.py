from nycodex import db
from nycodex.logging import get_logger
from nycodex.scrape import scrape_dataset, scrape_geojson
from nycodex.scrape.exceptions import SocrataError

BASE = "https://data.cityofnewyork.us/api"

logger = get_logger(__name__)


def main():
    session = db.Session()
    while True:
        try:
            with db.queue.next_row_to_scrape() as (trans, dataset_id):
                if dataset_id is None:
                    break
                # TODO(alan): Use same transaction connection for this query
                dataset_type, names, fields, types = session.query(
                    db.Dataset.asset_type, db.Dataset.column_names,
                    db.Dataset.column_sql_names, db.Dataset.column_types
                ).filter(db.Dataset.id == dataset_id).first()   # yapf: disable

                log = logger.bind(
                    dataset_id=dataset_id, dataset_type=dataset_type)
                log.info(f"Scraping dataset {dataset_id}")

                if dataset_type == db.AssetType.DATASET or names:
                    scrape_dataset(trans, dataset_id, names, fields, types)
                elif dataset_type == db.AssetType.MAP:
                    scrape_geojson(trans, dataset_id)
                else:
                    log.warning("Illegal dataset_type")
        except SocrataError as e:
            log.error("Failed to import dataset", exc_info=e)
        except Exception as e:
            log.critical(
                "Failed to import datset with unknown exception", exc_info=e)


if __name__ == "__main__":
    main()
