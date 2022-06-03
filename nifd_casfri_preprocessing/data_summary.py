import enum
from typing import Union
import pandas as pd
import sqlalchemy as sa
from IPython.display import display
from IPython.display import Markdown
from nifd_casfri_preprocessing import sql
import matplotlib.pyplot as plt
from nifd_casfri_preprocessing import log_helper

logger = log_helper.get_logger()
_cas_analysis_cols = [
    "stand_structure",
    "num_of_layers",
    "stand_photo_year",
]
_lyr_analysis_cols = [
    "soil_moist_reg",
    "structure_per",
    "structure_range",
    "crown_closure_upper",
    "crown_closure_lower",
    "height_upper",
    "height_lower",
    "productivity",
    "productivity_type",
    "origin_upper",
    "origin_lower",
    "site_class",
    "site_index",
]
_lyr_species_components_cols = [
    "species_1",
    "species_per_1",
    "species_2",
    "species_per_2",
    "species_3",
    "species_per_3",
    "species_4",
    "species_per_4",
    "species_5",
    "species_per_5",
    "species_6",
    "species_per_6",
    "species_7",
    "species_per_7",
    "species_8",
    "species_per_8",
    "species_9",
    "species_per_9",
    "species_10",
    "species_per_10",
]
_eco_analysis_cols = [
    "wetland_type",
    "wet_veg_cover",
    "wet_landform_mod",
    "wet_local_mod",
    "eco_site",
]
_nfl_analysis_cols = [
    "soil_moist_reg",
    "structure_per",
    "crown_closure_upper",
    "crown_closure_lower",
    "height_upper",
    "height_lower",
    "nat_non_veg",
    "non_for_anth",
    "non_for_veg",
]
_dst_analysis_cols = [
    "dist_type_1",
    "dist_year_1",
    "dist_ext_upper_1",
    "dist_ext_lower_1",
    "dist_type_2",
    "dist_year_2",
    "dist_ext_upper_2",
    "dist_ext_lower_2",
    "dist_type_3",
    "dist_year_3",
    "dist_ext_upper_3",
    "dist_ext_lower_3",
]


class DatabaseType(enum.Enum):
    casfri_postgres = 0
    geopackage = 1


def _sql_func(
    table_name: str, database_type: Union[int, DatabaseType], inventory_id: str
):
    database_type = DatabaseType(database_type)
    if database_type == DatabaseType.casfri_postgres:
        return sql.get_inventory_id_fitered_query(table_name, inventory_id)
    elif database_type == DatabaseType.geopackage:
        return sql.get_unfiltered_query(table_name)
    raise ValueError()


def _merge_area(df: pd.DataFrame, cas_df: pd.DataFrame) -> pd.DataFrame:
    return df.merge(cas_df[["cas_id", "casfri_area"]])


def load_data(
    engine: sa.engine.Engine,
    database_type: Union[int, DatabaseType],
    inventory_id: str,
) -> dict[str, pd.DataFrame]:
    data = {}
    for name in sql.NAMES:
        query = _sql_func(name, database_type, inventory_id)
        logger.info(f"query: {query}")
        data[name] = pd.read_sql(query, engine)

    for name in ["lyr", "eco", "nfl", "dst"]:
        data[name] = _merge_area(data[name], data["cas"])

    return data


def compile_summary(data: dict[str, pd.DataFrame]) -> dict:
    hdr = data["hdr"]
    cas = data["cas"]
    eco = data["eco"]
    lyr = data["lyr"]
    nfl = data["nfl"]
    dst = data["dst"]

    logger.info("compiling summaries")

    summary = {
        "hdr": hdr,
        "cas": {},
        "eco": {},
        "lyr": {},
        "lyr_species": {},
        "nfl": {},
        "dst": {},
    }
    logger.info("cas")
    for cas_analysis_col in _cas_analysis_cols:
        summary["cas"][cas_analysis_col] = (
            cas[[cas_analysis_col, "casfri_area"]]
            .groupby(cas_analysis_col)
            .sum()
        )

    logger.info("eco")
    for eco_analysis_col in _eco_analysis_cols:
        summary["eco"][eco_analysis_col] = (
            eco[[eco_analysis_col, "casfri_area"]]
            .groupby(eco_analysis_col)
            .sum()
        )

    logger.info("lyr")
    lyr_layer_ids = list(lyr["layer"].unique())
    for layer_id in lyr_layer_ids:
        data = {}
        for lyr_analysis_col in _lyr_analysis_cols:
            data[lyr_analysis_col] = (
                lyr.loc[lyr.layer == layer_id][
                    [lyr_analysis_col, "casfri_area"]
                ]
                .groupby(lyr_analysis_col)
                .sum()
            )
        summary["lyr"][layer_id] = data

    logger.info("lyr species")
    lyr_layer_ids = list(lyr["layer"].unique())
    for layer_id in lyr_layer_ids:
        data = {}
        for lyr_analysis_col in _lyr_species_components_cols:
            data[lyr_analysis_col] = (
                lyr.loc[lyr.layer == layer_id][
                    [lyr_analysis_col, "casfri_area"]
                ]
                .groupby(lyr_analysis_col)
                .sum()
            )
        summary["lyr_species"][layer_id] = data

    logger.info("nfl")
    nfl_layer_ids = list(nfl["layer"].unique())
    for layer_id in nfl_layer_ids:

        data = {}
        for nfl_analysis_col in _nfl_analysis_cols:
            data[nfl_analysis_col] = (
                nfl.loc[nfl.layer == layer_id][
                    [nfl_analysis_col, "casfri_area"]
                ]
                .groupby(nfl_analysis_col)
                .sum()
            )
        summary["nfl"][layer_id] = data

    logger.info("dst")
    dst_layer_ids = list(dst["layer"].unique())
    for layer_id in dst_layer_ids:

        data = {}
        for dst_analysis_col in _dst_analysis_cols:
            data[dst_analysis_col] = (
                dst.loc[dst.layer == layer_id][
                    [dst_analysis_col, "casfri_area"]
                ]
                .groupby(dst_analysis_col)
                .sum()
            )
        summary["dst"][layer_id] = data

    return summary


def display_summary(inventory_id: str, summary: dict) -> None:
    display(Markdown(f"## {inventory_id} hdr summary"))
    display(summary["hdr"].transpose())

    for k in summary["dst"].keys():
        display(
            Markdown(
                f"## {inventory_id} casfri dst table summary (layer id {k})"
            )
        )
        fig, axes = plt.subplots(nrows=3, ncols=4, figsize=(15, 15))
        for idx, (name, df) in enumerate(summary["dst"][k].items()):
            df.plot(ax=axes[idx // 4, idx % 4], kind="bar")
            fig.suptitle(
                f"{inventory_id} casfri dst (layer id {k})", fontsize=16
            )
        plt.tight_layout()
        plt.show()

    for k in summary["nfl"].keys():
        display(
            Markdown(
                f"## {inventory_id} casfri nfl table summary (layer id {k})"
            )
        )
        fig, axes = plt.subplots(nrows=3, ncols=3, figsize=(15, 15))
        for idx, (name, df) in enumerate(summary["nfl"][k].items()):
            df.plot(ax=axes[idx // 3, idx % 3], kind="bar")
        plt.tight_layout()
        plt.show()

    for k in summary["lyr"].keys():
        display(
            Markdown(
                f"## {inventory_id} casfri lyr table summary (layer id {k})"
            )
        )
        fig, axes = plt.subplots(nrows=5, ncols=3, figsize=(15, 15))
        for idx, (name, df) in enumerate(summary["lyr"][k].items()):
            df.plot(ax=axes[idx // 3, idx % 3], kind="bar")
        plt.tight_layout()
        plt.show()

    for k in summary["lyr_species"].keys():
        display(
            Markdown(
                f"## {inventory_id} casfri lyr (species) "
                f"table summary (layer id {k})"
            )
        )
        fig, axes = plt.subplots(nrows=10, ncols=2, figsize=(15, 40))
        for idx, (name, df) in enumerate(summary["lyr_species"][k].items()):
            df.plot(ax=axes[idx // 2, idx % 2], kind="bar")
        plt.tight_layout()
        plt.show()

    fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(10, 15))
    display(Markdown(f"## {inventory_id} casfri eco table summary"))
    for idx, (name, df) in enumerate(summary["eco"].items()):
        df.plot(ax=axes[idx // 2, idx % 2], kind="bar")

    plt.tight_layout()
    plt.show()

    fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(10, 15))
    display(Markdown(f"## {inventory_id} casfri cas table summary"))
    for idx, (name, df) in enumerate(summary["cas"].items()):
        df.plot(ax=axes[idx % 3], kind="bar")
    plt.tight_layout()
    plt.show()

    plt.close("all")
