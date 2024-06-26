import os
import enum
from typing import Union
import pandas as pd
import sqlite3
from sqlalchemy.engine.url import URL
from osgeo import gdal
import subprocess
from nifd_casfri_preprocessing import log_helper
from nifd_casfri_preprocessing import sql

logger = log_helper.get_logger()


class DatabaseType(enum.Enum):
    casfri_postgres = 0
    geopackage = 1


def get_sqlachemy_url(
    drivername: str,
    username: str,
    password: str,
    host: str,
    port: str,
    database: str,
) -> URL:
    config = dict(
        drivername=drivername,
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )

    url = URL.create(**config)
    return url


def get_gdal_pg_connection_info(
    username: str, password: str, host: str, port: str, database: str
) -> str:
    return (
        f"PG:host={host} dbname={database} port={port} "
        f"user={username} password={password}"
    )


def _vacuum_sqlite(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("VACUUM")
    conn.close()


def extract_to_geopackage(
    username: str,
    password: str,
    host: str,
    port: str,
    database: str,
    output_dir: str,
    inventory_id: str,
) -> None:
    output_path = os.path.join(output_dir, f"casfri_{inventory_id}.gpkg")
    if os.path.exists(output_path):
        os.unlink(output_path)
    connection_str = get_gdal_pg_connection_info(
        username, password, host, port, database
    )
    for idx, name in enumerate(sql.NAMES):

        args = [
            "ogr2ogr",
            "-f",
            "GPKG",
            output_path,
            connection_str,
            "-nln",
            name,
            "-sql",
            sql.get_inventory_id_fitered_query(name, inventory_id),
        ]
        if idx == 0:
            args.append("-overwrite")
        else:
            args.append("-update")
        logger.info(f"calling: {args}")
        subprocess.check_call(args)
        logger.info("running sqlite vacuum")
        _vacuum_sqlite(output_path)


def extract_to_parquet_with_raster(
    username: str,
    password: str,
    host: str,
    port: str,
    database: str,
    output_dir: str,
    inventory_id: str,
    resolution: float,
) -> None:
    url = str(
        get_sqlachemy_url(
            "postgresql", username, password, host, port, database
        )
    )
    _extract_parquet(output_dir, inventory_id, url)
    raster_path = os.path.join(output_dir, "cas_id.tiff")

    logger.info("calling gdal.Rasterize")
    gdal.Rasterize(
        destNameOrDestDS=os.path.join(output_dir, "cas_id.tiff"),
        srcDS=get_gdal_pg_connection_info(
            username, password, host, port, database
        ),
        options=gdal.RasterizeOptions(
            SQLStatement=sql.get_inventory_id_fitered_query(
                "gdal_rasterization", inventory_id
            ),
            attribute="raster_id",
            xRes=resolution,
            yRes=resolution,
            creationOptions=["BIGTIFF=YES", "COMPRESS=DEFLATE"],
            noData=-1,
            outputType=gdal.GDT_Int32,
        ),
    )

    # also create a wgs84 version of the raster
    wgs84_raster_path = os.path.join(output_dir, "cas_id_wgs84.tiff")
    logger.info("calling gdal.Warp")
    gdal.Warp(
        destNameOrDestDS=wgs84_raster_path,
        srcDSOrSrcDSTab=raster_path,
        options=gdal.WarpOptions(
            dstSRS="+proj=longlat +ellps=WGS84",
            creationOptions=["BIGTIFF=YES", "COMPRESS=DEFLATE"],
            outputType=gdal.GDT_Int32,
        ),
    )


def _extract_parquet(output_dir, inventory_id, url):
    data = load_data(url, DatabaseType.casfri_postgres, inventory_id)
    geo_lookup_query = sql.get_inventory_id_fitered_query(
        "gdal_rasterization_lookup", inventory_id
    )
    logger.info(f"query: {geo_lookup_query}")
    data["geo_lookup"] = pd.read_sql(geo_lookup_query, url)
    save_raw_tables(data, output_dir)


def _sql_func(
    table_name: str, database_type: Union[int, DatabaseType], inventory_id: str
):
    database_type = DatabaseType(database_type)
    if database_type == DatabaseType.casfri_postgres:
        return sql.get_inventory_id_fitered_query(table_name, inventory_id)
    elif database_type == DatabaseType.geopackage:
        return sql.get_unfiltered_query(table_name)
    raise ValueError()


def load_data(
    url: str,
    database_type: Union[int, DatabaseType],
    inventory_id: str,
) -> dict[str, pd.DataFrame]:
    data = {}
    for name in sql.NAMES:
        if name == "geo":
            continue
        query = _sql_func(name, database_type, inventory_id)
        logger.info(f"query: {query}")
        data[name] = pd.read_sql(query, url)

    return data


def load_parquet(data_dir: str) -> dict[str, pd.DataFrame]:
    data = {}
    for table in ["hdr", "cas", "eco", "lyr", "nfl", "dst", "geo_lookup"]:
        data[table] = pd.read_parquet(
            os.path.join(data_dir, f"{table}.parquet")
        )
    return data


def save_raw_tables(data: dict[str, pd.DataFrame], output_dir: str):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    for k, v in data.items():
        v.to_parquet(os.path.join(output_dir, f"{k}.parquet"), index=False)
