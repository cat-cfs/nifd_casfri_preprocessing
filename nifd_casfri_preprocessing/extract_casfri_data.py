import os
import subprocess


def get_pg_connection_info() -> str:
    return "host=localhost dbname=nifd port=6666 user=casfri password=casfri"


def extract(output_dir: str, inventory_id: str) -> None:
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    queries = {
        "hdr": f"SELECT * FROM hdr_all where inventory_id = {inventory_id}",
        "cas": f"SELECT * FROM cas_all where inventory_id = {inventory_id}",
        "geo": f"SELECT * FROM geo_all inner join cas_all on cas_all.cas_id = geo_all.cas_id where cas_all.inventory_id = {inventory_id}",
        "dst": f"SELECT * FROM dst_all inner join cas_all on cas_all.cas_id = dst_all.cas_id where cas_all.inventory_id = {inventory_id}",
        "eco": f"SELECT * FROM eco_all inner join cas_all on cas_all.cas_id = eco_all.cas_id where cas_all.inventory_id = {inventory_id}",
        "lyr": f"SELECT * FROM lyr_all inner join cas_all on cas_all.cas_id = lyr_all.cas_id where cas_all.inventory_id = {inventory_id}",
        "nfl": f"SELECT * FROM nfl_all inner join cas_all on cas_all.cas_id = nfl_all.cas_id where cas_all.inventory_id = {inventory_id}",
    }
    for idx, (name, query) in enumerate(queries.items()):
        args = [
            "ogr2ogr",
            "-f",
            '"GPKG"',
            os.path.join(output_dir, f"casfri_{inventory_id}.gpkg"),
            f"PG:{get_pg_connection_info()}",
            "-nln",
            name,
            "-sql",
            query,
        ]
        if idx == 0:
            args.append("-overwrite")
        else:
            args.append("-update")
        subprocess.check_call(args)
