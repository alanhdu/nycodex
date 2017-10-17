import hashlib
import typing

import dateutil.parser
import pytz
import requests

from nycodex import db

BASE = "https://api.us.socrata.com/api/catalog/v1"
DOMAIN = "data.cityofnewyork.us"


def api(path: str, params: typing.Dict[str, str] = None) -> requests.Response:
    if params is None:
        params = {}
    params.update({
        "domains": DOMAIN,
        "search_context": DOMAIN,
    })

    if path:
        url = "{}/{}".format(BASE, path)
    else:
        url = BASE
    return requests.get(url, params=params)


def shorten(s: str) -> str:
    if len(s) < 63:
        return s
    beginning = s[:30]
    ending = hashlib.md5(s.encode()).hexdigest()    # 32 chars
    return f"{beginning}-{ending}"


def get_facets():
    url = "{}/domains/{}/facets".format(BASE, DOMAIN)
    facets = requests.get(url, params={"limit": 10000}).json()
    for facet in facets:  # noqa
        pass


def main():
    db.Base.metadata.create_all(db.engine)

    owners = {}
    datasets = {}

    for category in db.DomainCategory.__members__.values():
        r = api("", params={"categories": category.value, "limit": 10000})
        for result in r.json()['results']:
            owner = result['owner']
            classification = result['classification']
            resource = result['resource']
            domain_metadata = {
                metadata['key']: metadata['value']
                for metadata in classification['domain_metadata']
            }

            owners[owner['id']] = db.Owner(
                id=owner['id'], name=owner['display_name'])
            assert resource['provenance'] in {"official", 'community'}

            assert 1 == len({
                len(resource['columns_name']),
                len(resource['columns_field_name']),
                len(resource['columns_description']),
                len(resource['columns_datatype'])
            })

            datasets[resource['id']] = db.Dataset(
                asset_type=resource['type'],
                attribution=resource['attribution'],
                dataset_agency=domain_metadata['Dataset-Information_Agency'],
                description=resource['description'],
                categories=classification['categories'],
                created_at=resource['createdAt'],
                domain_category=classification['domain_category'],
                domain_tags=classification['domain_tags'],
                id=resource['id'],
                is_auto_updated=domain_metadata['Update_Automation'] == 'Yes',
                is_official=resource['provenance'] == 'official',
                name=resource['name'],
                owner_id=owner['id'],
                update_frequency=domain_metadata['Update_Update-Frequency'],
                updated_at=dateutil.parser.parse(
                    resource['updatedAt']).astimezone(pytz.utc),
                column_names=resource['columns_name'],
                column_field_names=resource['columns_field_name'],
                column_descriptions=resource['columns_description'],
                column_types=resource['columns_datatype'],
                column_sql_names=[
                    shorten(field) for field in resource['columns_field_name']
                ])

    print("INSERTING", len(datasets), "datasets")

    with db.engine.connect() as conn:
        db.Owner.upsert(conn, owners.values())
        db.Dataset.upsert(conn, datasets.values())


if __name__ == "__main__":
    main()
