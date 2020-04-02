from nycodex import db, inference
from nycodex.logging import get_logger
from nycodex.scrape import scrape
from nycodex.scrape.exceptions import SocrataError

BASE = "https://data.cityofnewyork.us/api"

logger = get_logger(__name__)


def scrape_socrata() -> None:
    log = logger.bind()
    while True:
        try:
            with db.engine.connect() as conn:
                with db.queue.next_row_to_scrape(conn) as (tconn, dataset_id):
                    log = logger.bind(dataset_id=dataset_id)
                    if dataset_id is None:
                        break
                    scrape(tconn, dataset_id)
                    inference.preprocess_dataset(tconn, dataset_id)
        except SocrataError as e:
            log.error("Failed to import dataset", exc_info=e)
        except Exception as e:
            log.critical(
                "Failed to import datset with unknown exception", exc_info=e
            )


def main():
    scrape_socrata()


if __name__ == "__main__":
    main()
