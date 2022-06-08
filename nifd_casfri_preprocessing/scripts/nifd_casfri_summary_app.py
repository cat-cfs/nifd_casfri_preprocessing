import os
import sys
import argparse
from nifd_casfri_preprocessing import log_helper
from nifd_casfri_preprocessing import data_summary
from nifd_casfri_preprocessing.notebooks import report_writer


def summary_app_main(args):
    parser = argparse.ArgumentParser(
        description=(
            "Create a summary of variables contained in a a single casfri nifd"
            "inventory"
        )
    )
    parser.add_argument(
        "--url",
        help="sqlalchemy compatible url for the nifd casfri db",
        required=True,
    )
    parser.add_argument(
        "--database_type",
        help="0 for postgres, 1 for geopackage",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--inventory_id_list",
        help=(
            "a list of casfri nifd inventory ids "
            "(eg '--inventory_id_list PE01 BC12')"
        ),
        required=True,
        nargs="+",
    )
    parser.add_argument(
        "--raw_table_dir",
        help=(
            "a writeable directory into which raw data will be written. "
            "Will be created if it does not already exist"
        ),
        required=True,
    )
    parser.add_argument(
        "--report_output_dir",
        help=(
            "a writeable directory into which raw data will be written. "
            "Will be created if it does not already exist"
        ),
        required=True,
    )

    args = parser.parse_args(args=args)
    if not os.path.exists(args.report_output_dir):
        os.makedirs(args.report_output_dir)
    log_helper.start_logging(args.report_output_dir, "INFO")
    logger = log_helper.get_logger()
    logger.info(vars(args))
    try:

        inventory_ids = args.inventory_id_list
        for inventory_id in inventory_ids:
            inventory_raw_dir = os.path.join(args.raw_table_dir, inventory_id)
            if not os.path.exists(inventory_raw_dir):
                os.makedirs(inventory_raw_dir)
            report_output_dir = os.path.join(
                args.report_output_dir, inventory_id
            )
            if not os.path.exists(report_output_dir):
                os.makedirs(report_output_dir)
            data = data_summary.load_data(
                args.url, args.database_type, inventory_id
            )
            data_summary.save_raw_tables(data, inventory_raw_dir)
            report_writer.generate_report(
                "summarize_casfri_inventory.md",
                os.path.join(report_output_dir, f"{inventory_id}"),
                parameters=dict(
                    inventory_id=inventory_id,
                    raw_data_path=inventory_raw_dir,
                    output_path=report_output_dir,
                ),
            )

    except Exception:
        log_helper.get_logger().exception("")


def main():
    summary_app_main(sys.argv[1:])


if __name__ == "__main__":
    main()
