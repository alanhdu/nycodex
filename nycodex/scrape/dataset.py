import os
import tempfile
import typing

import pandas as pd

from nycodex import db
from .exceptions import SocrataTypeError

BASE = "https://data.cityofnewyork.us/api"


def scrape_dataset(dataset_id, names, fields, types) -> None:
    if any(len(f) > 63 for f in fields):
        # TODO(alan): Handle really long column names
        return

    df = pd.read_csv(
        f"{BASE}/views/{dataset_id}/rows.csv?accessType=DOWNLOAD",
        dtype={
            name: str
            for name, ty in zip(names, types)
            if ty not in {db.DataType.NUMBER, db.DataType.CHECKBOX}
        })
    df = df[names]  # Reorder columns
    df.columns = fields  # replace with normalized names

    columns, df = dataset_columns(df, types)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.abspath(os.path.join(tmpdir, "data.csv"))
        df.to_csv(path, header=False, index=False)

        # Handle Postgresql permission denied errors
        os.chmod(tmpdir, 0o775)
        os.chmod(path, 0o775)

        print("INSERTING", dataset_id)
        with db.engine.begin() as conn:
            conn.execute(f"DROP TABLE IF EXISTS \"{dataset_id}\"")
            conn.execute(f"CREATE TABLE \"{dataset_id}\" ({columns})")
            conn.execute(f"""
            COPY "{dataset_id}"
            FROM '{path}'
            WITH CSV NULL AS ''
            """)
            conn.execute(f"""
            UPDATE dataset
            SET scraped_at = NOW()
            WHERE id = '{dataset_id}'
            """)


def dataset_columns(df: pd.DataFrame, types: typing.Iterable[str]
                    ) -> typing.Tuple[str, pd.DataFrame]:
    columns = []
    for field, ty in zip(df.columns, types):
        # TODO(alan): ENUM
        if ty == db.DataType.CALENDAR_DATE:
            ty = "TIMESTAMP WITHOUT TIME ZONE"
        elif ty == db.DataType.CHECKBOX:
            ty = "BOOLEAN"
        elif ty == db.DataType.DATE:
            ty = "TIMESTAMP WITH TIME ZONE"
        elif ty in {
                db.DataType.EMAIL, db.DataType.HTML, db.DataType.LOCATION,
                db.DataType.PHONE, db.DataType.TEXT, db.DataType.URL
        }:
            ty = "TEXT"
        elif ty == db.DataType.MONEY:
            ty = "MONEY"
        elif ty == db.DataType.NUMBER:
            if not pd.api.types.is_numeric_dtype(df[field]):
                raise SocrataTypeError(field, ty, df[field].dtype)
            elif pd.api.types.is_integer_dtype(df[field]):
                # TODO(alan): Handle nullable integers
                min, max = df[field].min(), df[field].max()
                if -32768 < min and max < 32767:
                    ty = "SMALLINT"
                elif -2147483648 < min and max < 2147483647:
                    ty = "INTEGER"
                else:
                    ty = "BIGINT"
            else:
                ty = "DOUBLE PRECISION"
        elif ty == db.DataType.PERCENT:
            ty = "NUMERIC(6, 3)"
            try:
                df[field] = df[field].str[:-1].astype(float)
            except ValueError as e:
                raise SocrataTypeError(field, ty, df[field].dtype) from e
        else:
            raise RuntimeError(f"Unknown datatype {ty}")

        columns.append((field, ty))
    columns = ", ".join(f"\"{name}\" {ty}" for name, ty in columns)
    return columns, df
