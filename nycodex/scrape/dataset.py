import os
import tempfile
from typing import Iterable, List, Tuple

import geopandas as gpd
import pandas as pd
import sqlalchemy as sa

from nycodex import db
from nycodex.logging import get_logger

from . import exceptions, utils

BASE = "https://data.cityofnewyork.us/api"
RAW_SCHEMA = 'raw'
NULL_VALUES = frozenset({"null", "", "n/a", "na", "nan", "none"})

logger = get_logger(__name__)


def scrape(conn: sa.engine.Connection, dataset_id: str) -> None:
    dataset = db.Dataset.get_by_id(conn, dataset_id)

    log = logger.bind(dataset_id=dataset.id, dataset_type=dataset.asset_type)
    log.info(f"Scraping dataset {dataset_id}")

    if dataset.column_names:
        scrape_dataset(conn, dataset.id, dataset.column_names,
                       dataset.column_sql_names, dataset.column_types)
        log.info("Successfully inserted")
    elif dataset.asset_type == db.AssetType.MAP.value:
        scrape_geojson(conn, dataset.id)
        log.info("Successfully inserted")
    else:
        log.warning("Illegal dataset_type")


def scrape_geojson(conn: sa.engine.Connection, dataset_id: str) -> None:
    log = logger.bind(dataset_id=dataset_id, method="scrape_geojson")

    params = {"method": "export", "format": "GeoJSON"}
    url = f"{BASE}/geospatial/{dataset_id}"
    with utils.download_file(url, params=params) as fname:
        try:
            df = gpd.read_file(fname)
        except ValueError as e:
            raise exceptions.SocrataParseError from e

    for column in df.columns:
        if column == 'geometry':
            continue
        # Bad type inference
        try:
            df[column] = df[column].astype(int)
            continue
        except (ValueError, TypeError):
            pass
        try:
            df[column] = df[column].astype(float)
            continue
        except (ValueError, TypeError):
            pass
        try:
            df[column] = pd.to_datetime(df[column])
            continue
        except (ValueError, TypeError):
            pass

    log.info("Inserting")

    del df["geometry"]

    trans = conn.begin()
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{RAW_SCHEMA}.{dataset_id}"')
        df.to_sql(
            f"{dataset_id}",
            conn,
            if_exists='replace',
            index=False,
            schema=RAW_SCHEMA,
            )
    except Exception:
        trans.rollback()
        raise
    trans.commit()


def scrape_dataset(conn, dataset_id, names, fields, types) -> None:
    assert all(len(f) <= 63 for f in fields)
    url = f"{BASE}/views/{dataset_id}/rows.csv"
    with utils.download_file(url, params={"accessType": "DOWNLOAD"}) as fname:
        try:
            df = pd.read_csv(fname, dtype={name: str for name in names})
        except pd.errors.ParserError as e:
            raise exceptions.SocrataParseError from e
    df = df[names]  # Reorder columns
    df.columns = fields  # replace with normalized names

    columns, df = dataset_columns(df, types)
    schema = ", ".join(
        f'"{name}" {ty}' for name, ty in zip(df.columns, columns))
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.abspath(os.path.join(tmpdir, "data.csv"))
        df.to_csv(path, header=False, index=False)

        # Handle Postgresql permission denied errors
        os.chmod(tmpdir, 0o775)
        os.chmod(path, 0o775)

        trans = conn.begin()
        conn.execute(f'DROP TABLE IF EXISTS {RAW_SCHEMA}."{dataset_id}"')
        conn.execute(f'CREATE TABLE {RAW_SCHEMA}."{dataset_id}" ({schema})')
        conn.execute(f"""
        COPY {RAW_SCHEMA}."{dataset_id}"
        FROM '{path}'
        WITH CSV NULL AS ''
        """)
        trans.commit()


def dataset_columns(df: pd.DataFrame,
                    types: Iterable[str]) -> Tuple[List[str], pd.DataFrame]:
    columns = []
    for field, ty in zip(df.columns, types):
        column = df[field]
        mask = column.isnull() | column.str.lower().isin(NULL_VALUES)
        column_nonull = column[~mask]

        sql_type, column_nonull = _dataset_column(column_nonull, ty, field)

        df[field][mask] = ""
        df[field][~mask] = column_nonull
        columns.append(sql_type)
    return columns, df


def _dataset_column(column: pd.Series, ty: str,
                    field: str) -> Tuple[str, pd.Series]:
    if ty == db.DataType.CALENDAR_DATE:
        return "DATE", column
    elif ty == db.DataType.CHECKBOX:
        return "BOOLEAN", column
    elif ty == db.DataType.DATE:
        return "TIMESTAMP WITH TIME ZONE", column
    elif ty in {
            db.DataType.EMAIL, db.DataType.HTML, db.DataType.LOCATION,
            db.DataType.PHONE, db.DataType.TEXT, db.DataType.URL
    }:
        return "TEXT", column
    elif ty == db.DataType.MONEY:
        return "MONEY", column
    elif ty == db.DataType.NUMBER:
        try:
            ncolumn = pd.to_numeric(column)
        except (ValueError, TypeError) as e:
            raise exceptions.SocrataTypeError(field, ty, column.dtype)

        if pd.api.types.is_integer_dtype(ncolumn):
            min, max = ncolumn.min(), ncolumn.max()
            if -32768 < min and max < 32767:
                return "SMALLINT", column
            elif -2147483648 < min and max < 2147483647:
                return "INTEGER", column
            else:
                return "BIGINT", column
        return "DOUBLE PRECISION", column
    elif ty == db.DataType.PERCENT:
        if (column.str[-1] != "%").any():
            raise exceptions.SocrataTypeError(field, ty, column.dtype)
        try:
            column = pd.to_numeric(column.str[:-1])
        except (ValueError, TypeError) as e:
            raise exceptions.SocrataTypeError(field, ty, column.dtype)
        return "NUMERIC(6, 3)", column
    else:
        raise RuntimeError(f"Unknown datatype {ty}")
