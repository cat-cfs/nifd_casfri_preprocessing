import os
import sys
import argparse
import time
from nifd_casfri_preprocessing import log_helper
from nifd_casfri_preprocessing import process_for_cbm


def process_app_main(args):
    parser = argparse.ArgumentParser(
        description=(
            "Process extracted casfri parquet dataset into selected CBM inputs"
        )
    )
    parser.add_argument(
        "--data_dir",
        help=(
            "directory containing an extracted casfri parquet inventory "
            "dataset"
        ),
        required=True,
        type=os.path.abspath,
    )

    parser.add_argument(
        "--out_dir",
        help=("the output directory for this script"),
        required=True,
        type=os.path.abspath,
    )

    parser.add_argument(
        "--age_relative_year",
        help=(
            "The reference calendar year for the age raster produced by "
            "this script. Eg '2022'"
        ),
        type=int,
        required=True,
    )

    parser.add_argument(
        "--wgs84",
        help=(
            "flag, if set, indicates that processed rasters should be in "
            "wgs84 projection (required by GCBM) If not specified, the "
            "output is left in the default casfri projection. "
        ),
        required=False,
        action="store_true",
    )

    args = parser.parse_args(args=args)
    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)
    log_helper.start_logging(args.out_dir, "INFO")
    logger = log_helper.get_logger()
    logger.info(vars(args))
    try:
        start_time = time.time()
        log_helper.get_logger().info("process start")
        process_for_cbm.process(
            data_dir=args.data_dir,
            wgs84=args.wgs84,
            age_relative_year=args.age_relative_year,
            out_dir=args.out_dir,
        )
    except Exception:
        log_helper.get_logger().exception("")
    log_helper.get_logger().info(
        f"process end. Run time: {time.time() - start_time}"
    )


def main():
    process_app_main(sys.argv[1:])


if __name__ == "__main__":
    main()
