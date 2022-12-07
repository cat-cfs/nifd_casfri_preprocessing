import os
import sys
import argparse
import time
from nifd_casfri_preprocessing import casfri_data
from nifd_casfri_preprocessing import log_helper


def extract_main(args):
    parser = argparse.ArgumentParser(
        description=(
            "Extract a single inventory from the nfid casfri db into "
            "`GeoPackage`, or `parquet`"
        )
    )
    for db_info in ["host", "port", "database", "username", "password"]:
        parser.add_argument(
            f"--{db_info}", help="database connection info", required=True
        )
    parser.add_argument(
        "--inventory_id",
        help="The inventory id within the casfri db to extract. Eg. 'AB01'",
        required=True,
    )
    parser.add_argument(
        "--output_dir",
        help="The directory into which to write the geopackage",
        required=True,
    )
    parser.add_argument(
        "--output_format",
        help="the output format: can be one of `GeoPackage` or `parquet`",
        required=True,
    )

    parser.add_argument(
        "--resolution",
        help=(
            "the rasterization resolution in metres, this is required "
            "if output_format is `parquet`"
        ),
        required=False,
    )
    args = parser.parse_args(args=args)
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    log_helper.start_logging(args.output_dir, "INFO")
    try:
        start_time = time.time()
        log_helper.get_logger().info("process start")
        if args.output_format.lower() == "geopackage":
            casfri_data.extract_to_geopackage(
                args.username,
                args.password,
                args.host,
                args.port,
                args.database,
                args.output_dir,
                args.inventory_id,
            )
        elif args.output_format.lower() == "parquet":
            if not args.resolution:
                raise ValueError(
                    "resolution required when output_format is parquet"
                )
            casfri_data.extract_to_parquet_with_raster(
                args.username,
                args.password,
                args.host,
                args.port,
                args.database,
                args.output_dir,
                args.inventory_id,
                args.resolution,
            )

    except Exception:
        log_helper.get_logger().exception("")
    log_helper.get_logger().info(
        f"process end. Run time: {time.time() - start_time}"
    )


def main():
    extract_main(sys.argv[1:])


if __name__ == "__main__":
    main()
