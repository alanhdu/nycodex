from collections import OrderedDict

import pandas as pd
import pytest

from nycodex import db
from nycodex.scrape.dataset import dataset_columns
from nycodex.scrape.exceptions import SocrataTypeError


def test_dataset_columns_dtype_inference():
    df = pd.DataFrame(
        OrderedDict([
            ("SMALLINT", ["1", "2", "3", "4"]),
            ("INTEGER", ['1', '2', '3', '2147483646']),
            ("BIGINT", ['1', '2', '3', '2147483648']),
            ("DOUBLE PRECISION", ['1.0', '2.0', '3.0', '4.0']),
            ("BOOLEAN", ["True", "False", "True", "False"]),
            ("MONEY", ["$1", "$2.00", "$3.00", "$0"]),
            ("TEXT", ["a", "b", "c", "d"]),
            ("DATE", ["a", "b", "c", "d"]),
            ("TIMESTAMP WITH TIME ZONE", ["a", "b", "c", "d"]),
        ]))
    types = [
        db.DataType.NUMBER,
        db.DataType.NUMBER,
        db.DataType.NUMBER,
        db.DataType.NUMBER,
        db.DataType.CHECKBOX,
        db.DataType.MONEY,
        db.DataType.TEXT,
        db.DataType.CALENDAR_DATE,
        db.DataType.DATE,
    ]

    columns, new = dataset_columns(df, types)
    assert (columns == df.columns).all()
    pd.testing.assert_frame_equal(new, df)


def test_dataset_columns_percent():
    df = pd.DataFrame({"NUMERIC(6, 3)": ["1%", "100%", "23.02%"]})
    types = [db.DataType.PERCENT]

    columns, new = dataset_columns(df, types)
    assert (columns == df.columns).all()


def test_dataset_columns_number_error():
    df = pd.DataFrame({"a": ["1", "2", "3", "4", "hello"]})
    types = [db.DataType.NUMBER]

    with pytest.raises(SocrataTypeError):
        dataset_columns(df, types)


def test_dataset_columns_percent_error1():
    df = pd.DataFrame({"a": ["1", "2", "3", "4"]})
    types = [db.DataType.PERCENT]

    with pytest.raises(SocrataTypeError):
        dataset_columns(df, types)


def test_dataset_columns_percent_error2():
    df = pd.DataFrame({"a": ["3%", "a%"]})
    types = [db.DataType.PERCENT]

    with pytest.raises(SocrataTypeError):
        dataset_columns(df, types)
