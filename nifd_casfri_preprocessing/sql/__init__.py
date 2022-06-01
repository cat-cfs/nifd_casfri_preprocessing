import os

NAMES: list[str] = [
    "hdr",
    "cas",
    "geo",
    "dst",
    "eco",
    "lyr",
    "nfl",
]


def _get_script_dir() -> str:
    return os.path.dirname(os.path.realpath(__file__))


def get_inventory_id_fitered_query(name: str, inventory_id: int) -> str:
    with open(os.path.join(_get_script_dir(), f"{name}.sql")) as fp:
        return fp.read().format(inventory_id=int(inventory_id))
