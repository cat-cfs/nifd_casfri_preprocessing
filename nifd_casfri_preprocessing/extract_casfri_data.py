import os
import sys
import argparse
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


def extract_main(args):
    parser = argparse.ArgumentParser(
        description=(
            "Extract a single inventory from the nfid casfri db as a "
            "`GeoPackage`"
        )
    )
    parser.add_argument(
        "--inventory_id",
        help="The inventory id within the casfri db to extract. Eg. 'AB01'",
    )
    parser.add_argument(
        "--output_dir", help="The directory into which to write the geopackage"
    )
    args = parser.parse_args(args=args)
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    log_helper.start_logging(args.output_dir, "INFO")
    try:
        extract(args.output_dir, args.inventory_id)
    except Exception:
        log_helper.get_logger().exception("")


def main():
    extract_main(sys.argv[1:])


if __name__ == "__main__":
    main()
