import os

NAMES = [
    "hdr",
    "cas",
    "geo",
    "dst",
    "eco",
    "lyr",
    "nfl",
]


def _get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))


def get_inventory_id_fitered_query(name, inventory_id):
    with open(os.path.join(_get_script_dir(), "{name}.sql")) as fp:
        return fp.read().format(inventory_id)
