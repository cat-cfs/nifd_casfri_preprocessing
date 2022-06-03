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


def get_inventory_id_fitered_query(name: str, inventory_id: str) -> str:
    with open(os.path.join(_get_script_dir(), f"{name}.sql")) as fp:
        return fp.read().format(inventory_id=inventory_id)


def get_unfiltered_query(name: str):
    if name not in NAMES:
        raise ValueError()
    return f"SELECT * from {name}"
