import os
import sys
import argparse
from nifd_casfri_preprocessing import casfri_data
from nifd_casfri_preprocessing import log_helper


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
        required=True,
    )
    parser.add_argument(
        "--output_dir",
        help="The directory into which to write the geopackage",
        required=True,
    )
    args = parser.parse_args(args=args)
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    log_helper.start_logging(args.output_dir, "INFO")
    try:
        casfri_data.extract_to_geopackage(args.output_dir, args.inventory_id)
    except Exception:
        log_helper.get_logger().exception("")


def main():
    extract_main(sys.argv[1:])


if __name__ == "__main__":
    main()
