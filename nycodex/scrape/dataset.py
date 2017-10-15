import os
import tempfile
import typing

import geoalchemy2
import geopandas as gpd
import pandas as pd
import requests

from nycodex import db
from nycodex.logging import get_logger
from . import exceptions

BASE = "https://data.cityofnewyork.us/api"

logger = get_logger(__name__)


def scrape_geojson(dataset_id: str) -> None:
    log = logger.bind(dataset_id=dataset_id, method="scrape_geojson")

    params = {"method": "export", "format": "GeoJSON"}
    r = requests.get(f"{BASE}/geospatial/{dataset_id}", params=params)

    with tempfile.NamedTemporaryFile() as fout:
        fout.write(r.content)
        fout.flush()

        df = gpd.read_file(fout.name)

    for column in df.columns:
        if column == 'geometry':
            continue
        # Bad type inference
        try:
            df[column] = df[column].astype(int)
            continue
        except ValueError:
            pass
        try:
            df[column] = df[column].astype(float)
            continue
        except ValueError:
            pass
        try:
            df[column] = pd.to_datetime(df[column])
            continue
        except ValueError:
            pass

    log.info("Inserting")

    # TODO: Use ogr2ogr2?
    # srid 4326 for latitude/longitude coordinates
    ty = df.geometry.map(lambda x: x.geometryType()).unique()
    assert len(ty) == 1
    ty = ty[0]

    df['geometry'] = df['geometry'].map(
        lambda x: geoalchemy2.WKTElement(x.wkt, srid=4326))
    df.to_sql(
        f"{dataset_id}-new",
        db.engine,
        if_exists='replace',
        index=False,
        dtype={"geometry": geoalchemy2.Geometry(geometry_type=ty, srid=4326)})

    with db.engine.begin() as trans:
        trans.execute(f"DROP TABLE IF EXISTS\"{dataset_id}\"")
        trans.execute(f"""
        ALTER TABLE \"{dataset_id}-new\"
        RENAME TO "{dataset_id}"
        """)
        trans.execute(f"""
        UPDATE dataset
        SET scraped_at = NOW()
        WHERE id = '{dataset_id}'
        """)
    log.info("Successfully inserted")


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
