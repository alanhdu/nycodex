import os
import json
import gzip

from nycodex import db, scrape

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")

def test_json_to_dataset():
    with gzip.open(os.path.join(FIXTURES, "datasets.json.gz")) as fin:
        data = json.load(fin)

    for result in data["results"]:
        data = scrape.json_to_dataset(result)
        assert data.asset_type in {key.name for key in db.AssetType}
