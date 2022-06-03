import enum
import pandas as pd
import sqlalchemy as sa
from IPython.display import display
from IPython.display import Markdown
from nifd_casfri_preprocessing import sql
import matplotlib.pyplot as plt


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


def _sql_func(table_name: str, type: DatabaseType, inventory_id: str):
    if type == DatabaseType.casfri_postgres:
        return sql.get_inventory_id_fitered_query(table_name, inventory_id)
    elif type == DatabaseType.geopackage:
        return sql.get_unfiltered_query(table_name)


def _merge_area(df: pd.DataFrame, cas_df: pd.DataFrame) -> pd.DataFrame:
    return df.merge(cas_df[["cas_id", "casfri_area"]])


def _load_data(
    engine: sa.engine.Engine, database_type: DatabaseType, inventory_id: str
) -> dict[str, pd.DataFrame]:
    data = {}
    for name in sql.NAMES:
        data[name] = pd.read_sql(_sql_func(name, type, inventory_id))

    for name in ["lyr", "eco", "nfl", "dst"]:
        data[name] = _merge_area(data[name], data["cas"])

    return data


def _compile_summary(
    cas: pd.DataFrame,
    eco: pd.DataFrame,
    lyr: pd.DataFrame,
    nfl: pd.DataFrame,
    dst: pd.DataFrame,
) -> dict:
    analysis = {
        "cas": {},
        "eco": {},
        "lyr": {},
        "lyr_species": {},
        "nfl": {},
        "dst": {},
    }
    for cas_analysis_col in _cas_analysis_cols:
        analysis["cas"][cas_analysis_col] = (
            cas[[cas_analysis_col, "casfri_area"]]
            .groupby(cas_analysis_col)
            .sum()
        )

    for eco_analysis_col in _eco_analysis_cols:
        analysis["eco"][eco_analysis_col] = (
            eco[[eco_analysis_col, "casfri_area"]]
            .groupby(eco_analysis_col)
            .sum()
        )

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
        analysis["lyr"][layer_id] = data

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
        analysis["lyr_species"][layer_id] = data

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
        analysis["nfl"][layer_id] = data

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
        analysis["dst"][layer_id] = data

    return analysis


def get_data_summary(
    url: str, database_type: DatabaseType, inventory_id: str
) -> dict:
    engine = sa.create_engine(url)

    data = _load_data(engine, database_type, inventory_id)

    analysis = _compile_summary(
        data["cas"],
        data["eco"],
        data["lyr"],
        data["nfl"],
        data["dst"],
    )
    return analysis


def display(inventory_id: str, data: dict[str, pd.DataFrame], analysis: dict) ->None:
    for k in analysis["dst"].keys():
        fig, axes = plt.subplots(nrows=3, ncols=4, figsize=(15, 15))
        for idx, (name, df) in enumerate(analysis["dst"][k].items()):
            df.plot(ax=axes[idx // 4, idx % 4], kind="bar")
            fig.suptitle(
                f"{inventory_id} casfri dst (layer id {k})", fontsize=16
            )
            plt.tight_layout()

    display(Markdown(f"## {inventory_id} casfri nfl table summary (layer id {k})"))
    for k in analysis["nfl"].keys():
        fig, axes = plt.subplots(nrows=3, ncols=3, figsize=(15, 15))
        for idx, (name, df) in enumerate(analysis["nfl"][k].items()):
            df.plot(ax=axes[idx // 3, idx % 3], kind="bar")
            plt.tight_layout()

    for k in analysis["lyr"].keys():
        fig, axes = plt.subplots(nrows=5, ncols=3, figsize=(15, 15))
        for idx, (name, df) in enumerate(analysis["lyr"][k].items()):
            df.plot(ax=axes[idx // 3, idx % 3], kind="bar")
            fig.suptitle(
                f"{inventory_id} casfri lyr table summary (layer id {k})",
                fontsize=16,
            )
            plt.tight_layout()

    for k in analysis["lyr_species"].keys():
        fig, axes = plt.subplots(nrows=10, ncols=2, figsize=(15, 40))
        for idx, (name, df) in enumerate(analysis["lyr_species"][k].items()):
            df.plot(ax=axes[idx // 2, idx % 2], kind="bar")
            fig.suptitle(
                f"{inventory_id} casfri lyr (species) table summary (layer id {k})",
                fontsize=16,
            )
            plt.tight_layout()

    fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(10, 15))
    for idx, (name, df) in enumerate(analysis["eco"].items()):
        df.plot(ax=axes[idx // 2, idx % 2], kind="bar")
        fig.suptitle(f"{inventory_id} casfri eco table summary", fontsize=16)
        plt.tight_layout()

    fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(10, 15))
    for idx, (name, df) in enumerate(analysis["cas"].items()):
        df.plot(ax=axes[idx % 3], kind="bar")
        fig.suptitle(f"{inventory_id} casfri cas table summary", fontsize=16)
        plt.tight_layout()
