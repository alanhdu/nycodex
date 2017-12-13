import contextlib
import datetime as dt
import json
import os

import pandas as pd
import pytest

from nycodex import db
from nycodex.scrape import utils
from nycodex.scrape.dataset import scrape_dataset

fixtures = os.path.join(os.path.split(__file__)[0], "fixtures")


@pytest.mark.parametrize("dataset_id", os.listdir(fixtures))
def test_scrape_dataset(conn, dataset_id, mocker):
    with open(os.path.join(fixtures, dataset_id, "metadata.json")) as fin:
        metadata = json.load(fin)
    fields, names, types = (metadata['fields'], metadata['names'],
                            metadata['types'])
    fname = os.path.join(fixtures, dataset_id, "data.csv")

    @contextlib.contextmanager
    def fake(fname):
        yield fname

    with mocker.patch.object(utils, "download_file", return_value=fake(fname)):
        scrape_dataset(conn, dataset_id, names, fields, types)

    df = pd.read_sql(f'SELECT * FROM "raw.{dataset_id}"', conn)
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
