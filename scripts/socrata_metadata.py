from typing import Any, Dict

import requests

from nycodex import db, scrape
from nycodex.logging import get_logger

BASE = "https://api.us.socrata.com/api/catalog/v1"
DOMAIN = "data.cityofnewyork.us"


def api(params: Dict[str, Any]) -> requests.Response:
    params = {"domains": DOMAIN, "search_context": DOMAIN, **params}
    return requests.get(BASE, params=params)


def main() -> None:
    log = get_logger(__name__)

    db.Base.metadata.create_all(db.engine)

    log.info("Scraping dataset metadata")
    r = api(params={"limit": 10000})

    results = r.json()["results"]
    assert len(results) < 10000

    datasets = [scrape.json_to_dataset(result) for result in results]
    log.info(f"Inserting {len(datasets)} datasets")

    for dataset in datasets:
        with db.engine.begin() as conn:
            db.Dataset.upsert(conn, [dataset])
            db.Field.upsert(conn, dataset.fields)
    log.info("Complete!")


if __name__ == "__main__":
    main()
