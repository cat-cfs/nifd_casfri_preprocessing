import enum
import pandas as pd
import sqlalchemy as sa
from IPython.display import display

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


def get_data_summary(url: str, type: DatabaseType, inventory_id: str):
    engine = sa.create_engine(url)

    hdr = pd.read_sql("SELECT * from hdr", engine)
    cas = pd.read_sql("SELECT * from cas", engine)
    lyr = pd.read_sql("SELECT * from lyr", engine).merge(
        cas[["cas_id", "casfri_area"]]
    )
    eco = pd.read_sql("SELECT * from eco", engine).merge(
        cas[["cas_id", "casfri_area"]]
    )

    nfl = pd.read_sql("SELECT * from nfl", engine).merge(
        cas[["cas_id", "casfri_area"]]
    )
    dst = pd.read_sql("SELECT * from dst", engine).merge(
        cas[["cas_id", "casfri_area"]]
    )

    display(hdr.transpose())


def _compile_summary(
    cas: pd.DataFrame,
    eco: pd.DataFrame,
    lyr: pd.DataFrame,
    nfl: pd.DataFrame,
    dst: pd.DataFrame,
):
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
