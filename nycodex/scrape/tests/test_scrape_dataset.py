import contextlib
import datetime as dt
import json
import os

import geopandas as gpd
import pandas as pd
import pytest

from nycodex import db
from nycodex.scrape import utils
from nycodex.scrape.dataset import scrape_dataset, scrape_geojson

dataset_fixtures = os.path.join(
    os.path.split(__file__)[0], "fixtures/datasets")
geojson_fixtures = os.path.join(os.path.split(__file__)[0], "fixtures/geojson")


@pytest.mark.parametrize("dataset_id", os.listdir(dataset_fixtures))
def test_scrape_dataset(conn, dataset_id, mocker):
    with open(os.path.join(dataset_fixtures, dataset_id,
                           "metadata.json")) as fin:
        metadata = json.load(fin)
    fields, names, types = (metadata['fields'], metadata['names'],
                            metadata['types'])
    fname = os.path.join(dataset_fixtures, dataset_id, "data.csv")

    @contextlib.contextmanager
    def fake(fname):
        yield fname

    with mocker.patch.object(utils, "download_file", return_value=fake(fname)):
        scrape_dataset(conn, dataset_id, names, fields, types)

    df = pd.read_sql(f'SELECT * FROM raw."{dataset_id}"', conn)
    assert (df.columns == fields).all()

    for field, ty in zip(fields, types):
        if ty == db.DataType.CALENDAR_DATE:
            for d in df[field]:
                if d is not None:
                    assert isinstance(d, dt.date)
        elif ty == db.DataType.CHECKBOX:
            assert pd.api.types.is_bool_dtype(df[field])
        elif ty == db.DataType.DATE:
            assert pd.api.types.is_datetimetz(df[field])
        elif ty == db.DataType.MONEY:
            assert (df[field].str[0] == '$').all()
        elif ty in {db.DataType.NUMBER, db.DataType.PERCENT}:
            assert pd.api.types.is_numeric_dtype(df[field])


@pytest.mark.parametrize("dataset_id", os.listdir(geojson_fixtures))
def test_scrape_geojson(conn, dataset_id, mocker):
    fname = os.path.join(geojson_fixtures, dataset_id, "data.geojson")
    with open(os.path.join(geojson_fixtures, dataset_id,
                           "metadata.json")) as fin:
        metadata = json.load(fin)

    @contextlib.contextmanager
    def fake(fname):
        yield fname

    with mocker.patch.object(utils, "download_file", return_value=fake(fname)):
        scrape_geojson(conn, dataset_id)

    df = gpd.read_postgis(
        f'SELECT * FROM raw."{dataset_id}"', conn, geom_col="geometry")
    assert len(df) == metadata['num_rows']
    assert df['geometry'][0].type == metadata['geometry_type']
    assert len(df.columns) == len(metadata["columns"]) + 1
    for name, ty in metadata["columns"].items():
        assert df[name].dtype.kind == ty
