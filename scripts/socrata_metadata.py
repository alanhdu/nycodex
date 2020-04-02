import hashlib
from typing import Any, Dict, Optional

import dateutil.parser
import pytz
import requests

from nycodex import db
from nycodex.logging import get_logger

BASE = "https://api.us.socrata.com/api/catalog/v1"
DOMAIN = "data.cityofnewyork.us"


def api(
    path: str, params: Optional[Dict[str, Any]] = None
) -> requests.Response:
    if params is None:
        params = {}
    params.update(
        {"domains": DOMAIN, "search_context": DOMAIN,}
    )

    if path:
        url = "{}/{}".format(BASE, path)
    else:
        url = BASE
    return requests.get(url, params=params)


def shorten(s: str) -> str:
    if len(s) < 63:
        return s
    beginning = s[:30]
    ending = hashlib.md5(s.encode()).hexdigest()  # 32 chars
    return f"{beginning}-{ending}"


def get_facets() -> None:
    url = "{}/domains/{}/facets".format(BASE, DOMAIN)
    facets = requests.get(url, params={"limit": 10000}).json()
    for facet in facets:  # noqa
        pass


def parse_json(result: Dict[str, Any]) -> db.Dataset:
    owner = result["owner"]
    classification = result["classification"]
    resource = result["resource"]
    domain_metadata = {
        metadata["key"]: metadata["value"]
        for metadata in classification["domain_metadata"]
    }

    assert resource["provenance"] in {"official", "community"}

    assert 1 == len(
        {
            len(resource["columns_name"]),
            len(resource["columns_field_name"]),
            len(resource["columns_description"]),
            len(resource["columns_datatype"]),
        }
    )

    return db.Dataset(
        asset_type=resource["type"],
        attribution=resource["attribution"],
        categories=classification["categories"],
        column_descriptions=resource["columns_description"],
        column_field_names=resource["columns_field_name"],
        column_names=resource["columns_name"],
        column_sql_names=[
            shorten(field) for field in resource["columns_field_name"]
        ],
        column_types=resource["columns_datatype"],
        created_at=resource["createdAt"],
        dataset_agency=domain_metadata.get("Dataset-Information_Agency"),
        description=resource["description"],
        domain_category=classification["domain_category"],
        domain_tags=classification["domain_tags"],
        id=resource["id"],
        is_auto_updated=domain_metadata.get("Update_Automation") == "Yes",
        is_official=resource["provenance"] == "official",
        name=resource["name"],
        owner_id=owner["id"],
        page_views_last_month=resource["page_views"]["page_views_last_month"],
        page_views_last_week=resource["page_views"]["page_views_last_week"],
        page_views_total=resource["page_views"]["page_views_total"],
        parents=resource["parent_fxf"] if resource["parent_fxf"] else [],
        update_frequency=domain_metadata.get("Update_Update-Frequency"),
        updated_at=dateutil.parser.parse(resource["updatedAt"]).astimezone(
            pytz.utc
        ),
    )


def main() -> None:
    log = get_logger(__name__)

    db.Base.metadata.create_all(db.engine)

    log.info("Scraping datasets")
    datasets = {}
    for category in db.DomainCategory.__members__.values():
        r = api("", params={"categories": category.value, "limit": 10000})
        for result in r.json()["results"]:
            d = parse_json(result)
            datasets[d.id] = d

    log.info(f"Inserting {len(datasets)} datasets")
    with db.engine.connect() as conn:
        db.Dataset.upsert(conn, datasets.values())

        log.info(f"Adding jobs to queue")
        db.queue.update_from_metadata(conn)

    log.info("Complete!")


if __name__ == "__main__":
    main()
