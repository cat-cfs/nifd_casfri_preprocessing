import os
import enum
from typing import Union
import pandas as pd
import sqlite3
import sqlalchemy as sa
import subprocess
from nifd_casfri_preprocessing import log_helper
from nifd_casfri_preprocessing import sql

logger = log_helper.get_logger()


class DatabaseType(enum.Enum):
    casfri_postgres = 0
    geopackage = 1


def get_pg_connection_info() -> str:
    return "host=localhost dbname=nifd port=6666 user=casfri password=casfri"


def _vacuum_sqlite(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("VACUUM")
    conn.close()


def extract_to_geopackage(output_dir: str, inventory_id: str) -> None:
    output_path = os.path.join(output_dir, f"casfri_{inventory_id}.gpkg")
    if os.path.exists(output_path):
        os.unlink(output_path)
    for idx, name in enumerate(sql.NAMES):
        args = [
            "ogr2ogr",
            "-f",
            "GPKG",
            output_path,
            f"PG:{get_pg_connection_info()}",
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
    url: str,
    database_type: DatabaseType,
    output_dir: str,
    inventory_id: str,
    resolution: float,
) -> None:
    data = load_data(url, database_type, inventory_id)
    data["geo_lookup"] = pd.read_sql(
        sql.get_inventory_id_fitered_query(
            "gdal_rasterization_lookup", inventory_id
        ),
        url,
    )
    save_raw_tables(data, output_dir)

    rasterization_args = [
        "gdal_rasterize",
        "-a",
        "raster_id",
        "-tr",
        str(resolution),
        str(resolution),
        f"PG:{get_pg_connection_info()}",
        "-sql",
        sql.get_inventory_id_fitered_query("gdal_rasterization", inventory_id),
        "-co",
        "COMPRESS=DEFLATE",
        "-co",
        "BIGTIFF=YES",
        "-ot",
        "Int64",
        "-a_nodata",
        "-1",
        os.path.join(output_dir, f"{inventory_id}_cas_id.tiff"),
    ]
    logger.info(f"calling: {rasterization_args}")
    subprocess.check_call(rasterization_args)


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
    engine: sa.engine.Engine,
    database_type: Union[int, DatabaseType],
    inventory_id: str,
) -> dict[str, pd.DataFrame]:
    data = {}
    for name in sql.NAMES:
        if name == "geo":
            continue
        query = _sql_func(name, database_type, inventory_id)
        logger.info(f"query: {query}")
        data[name] = pd.read_sql(query, engine)

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
