import os
import tempfile
import typing

import pandas as pd

from nycodex import db
from nycodex.logging import get_logger
from . import exceptions

BASE = "https://data.cityofnewyork.us/api"

logger = get_logger(__name__)


def scrape_dataset(dataset_id, names, fields, types) -> None:
    log = logger.bind(dataset_id=dataset_id)

    for f in fields:
        if len(f) > 63:
            raise exceptions.SocrataColumnNameTooLong(f)

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
    columns = ", ".join(f"\"{name}\" {ty}"
                        for name, ty in zip(df.columns, columns))
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.abspath(os.path.join(tmpdir, "data.csv"))
        df.to_csv(path, header=False, index=False)

        # Handle Postgresql permission denied errors
        os.chmod(tmpdir, 0o775)
        os.chmod(path, 0o775)

        log.info("Inserting dataset")
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
        log.info("Insert Sucessful!")


def dataset_columns(df: pd.DataFrame, types: typing.Iterable[str]
                    ) -> typing.Tuple[typing.List[str], pd.DataFrame]:
    columns = []
    for field, ty in zip(df.columns, types):
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
                raise exceptions.SocrataTypeError(field, ty, df[field].dtype)
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
            if (df[field].dropna().str[-1] != "%").any():
                raise exceptions.SocrataTypeError(field, ty, df[field].dtype)
            try:
                df[field] = df[field].str[:-1].astype(float)
            except ValueError as e:
                raise exceptions.SocrataTypeError(field, ty, df[field].dtype)
        else:
            raise RuntimeError(f"Unknown datatype {ty}")
        columns.append(ty)
    return columns, df
