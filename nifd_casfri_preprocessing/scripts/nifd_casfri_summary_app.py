import os
import sys
import argparse
import time
from nifd_casfri_preprocessing import log_helper
from nifd_casfri_preprocessing.notebooks import report_writer


def summary_app_main(args):
    parser = argparse.ArgumentParser(
        description=(
            "Create a summary of variables contained in a a single casfri nifd"
            "inventory"
        )
    )
    parser.add_argument(
        "--inventory_id",
        help=("a casfri inventory id string (eg PE01)"),
        required=True,
    )
    parser.add_argument(
        "--raw_table_dir",
        help=(
            "a directory containing parquet formatted raw casfri attribute "
            "tables"
        ),
        required=True,
        type=os.path.abspath,
    )
    parser.add_argument(
        "--report_output_dir",
        help=(
            "a writeable directory into which raw data will be written. "
            "Will be created if it does not already exist"
        ),
        required=True,
        type=os.path.abspath,
    )

    args = parser.parse_args(args=args)
    if not os.path.exists(args.report_output_dir):
        os.makedirs(args.report_output_dir)
    log_helper.start_logging(args.report_output_dir, "INFO")
    logger = log_helper.get_logger()
    logger.info(vars(args))
    try:
        start_time = time.time()
        log_helper.get_logger().info("process start")
        if not os.path.exists(args.report_output_dir):
            os.makedirs(args.report_output_dir)
        report_writer.generate_report(
            "summarize_casfri_inventory.md",
            os.path.join(args.report_output_dir, f"{args.inventory_id}"),
            parameters=dict(
                inventory_id=args.inventory_id,
                raw_data_path=args.raw_table_dir,
                output_path=args.report_output_dir,
            ),
        )

    except Exception:
        log_helper.get_logger().exception("")
    log_helper.get_logger().info(
        f"process end. Run time: {time.time() - start_time}"
    )


def main():
    summary_app_main(sys.argv[1:])


if __name__ == "__main__":
    main()
