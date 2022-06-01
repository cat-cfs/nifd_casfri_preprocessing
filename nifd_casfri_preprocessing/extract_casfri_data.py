import os

import subprocess
from nifd_casfri_preprocessing import log_helper
from nifd_casfri_preprocessing import sql


def get_pg_connection_info() -> str:
    return "host=localhost dbname=nifd port=6666 user=casfri password=casfri"


def extract(output_dir: str, inventory_id: str) -> None:

    logger = log_helper.get_logger()
    for idx, name in enumerate(sql.NAMES):
        args = [
            "ogr2ogr",
            "-f",
            '"GPKG"',
            os.path.join(output_dir, f"casfri_{inventory_id}.gpkg"),
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
