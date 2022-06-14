import os

from typing import Union
import pandas as pd
from IPython.display import display
from IPython.display import Markdown

import matplotlib.pyplot as plt
from nifd_casfri_preprocessing import log_helper
from nifd_casfri_preprocessing import casfri_data

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


def _merge_area(df: pd.DataFrame, cas_df: pd.DataFrame) -> pd.DataFrame:
    return df.merge(cas_df[["cas_id", "casfri_area"]])


def clean_nulls(df: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    if df.index.is_numeric():
        null_value_area = float(df[df.index < -8000].sum())
        df = df[df.index >= -8000].copy()
    else:
        undefined_labels = ["NOT_APPLICABLE", "NULL_VALUE"]
        null_value_area = float(df[df.index.isin(undefined_labels)].sum())
        df = df[~df.index.isin(undefined_labels)].copy()
    if len(df.index) > 0:
        return df, null_value_area
    else:
        return None, null_value_area


class Summary:
    def __init__(self, data: dict[str, pd.DataFrame]):
        self._data = data
        self._table_areas: dict[str, float] = self._compute_table_area_totals()
        self._summary_data: dict[str, pd.DataFrame] = {}
        self._summary_data_cleaned: dict[str, pd.DataFrame] = {}
        self._null_summary: dict[str, float] = {}
        self._directory: dict[str, dict[Union[int, None], list[str]]] = {}
        self._compile_summary()

    def _compute_table_area_totals(self):
        areas = {}
        for table in ["cas", "eco", "lyr", "nfl", "dst"]:
            raw_table = self._data[table]
            areas[table] = (
                raw_table[["cas_id", "casfri_area"]]
                .drop_duplicates("cas_id")["casfri_area"]
                .sum()
            )
        return areas

    def insert(
        self, df: pd.DataFrame, table: str, column: str, layer_id: int = None
    ):

        if layer_id:
            key = f"{table}.layer_{layer_id}.{column}"
        else:
            key = key = f"{table}.{column}"
        self._insert_directory(table, layer_id, key)
        self._summary_data[key] = df
        df_cleaned, null_value_area = clean_nulls(df)
        self._null_summary[key] = null_value_area
        if df_cleaned is not None:
            self._summary_data_cleaned[key] = df_cleaned

    def _insert_directory(self, table: str, layer_id: int, key: str):
        if table not in self._directory:
            self._directory[table] = {}
        if layer_id not in self._directory[table]:
            self._directory[table][layer_id] = []
        self._directory[table][layer_id].append(key)

    def get_tables(self) -> list[str]:
        return list(self._directory.keys())

    def get_layers(self, table_name: str) -> list[Union[int, None]]:
        return list(self._directory[table_name].keys())

    def get_null_summary(
        self, table: str, layer_id: int = None
    ) -> Union[pd.DataFrame, None]:
        data = []
        for key in self._directory[table][layer_id]:
            if key in self._null_summary:
                total_area = self._table_areas[table]
                data.append([key, self._null_summary[key], total_area])
        if data:

            df = pd.DataFrame(
                columns=["col", "null_area", "total_area"], data=data
            ).set_index("col")
            df["percent_null"] = (df["null_area"] / df["total_area"]) * 100.0
            return df
        return None

    def get_summary_data(
        self, table: str, layer_id: int = None, cleaned: bool = True
    ) -> dict[str, pd.DataFrame]:
        output = {}
        for key in self._directory[table][layer_id]:
            if cleaned:
                if key in self._summary_data_cleaned:
                    output[key] = self._summary_data_cleaned[key]
            else:
                output[key] = self._summary_data[key]
        return output

    def get_raw_table(self, name) -> pd.DataFrame:
        return self._data[name]

    def _compile_summary(self):

        cas = self._data["cas"]
        eco = self._data["eco"]
        lyr = self._data["lyr"]
        nfl = self._data["nfl"]
        dst = self._data["dst"]

        logger.info("compiling summaries")

        logger.info("cas")

        for cas_analysis_col in _cas_analysis_cols:

            df = (
                cas[[cas_analysis_col, "casfri_area"]]
                .groupby(cas_analysis_col)
                .sum()
            )
            self.insert(df, "cas", cas_analysis_col, None)

        logger.info("eco")
        for eco_analysis_col in _eco_analysis_cols:
            df = (
                eco[[eco_analysis_col, "casfri_area"]]
                .groupby(eco_analysis_col)
                .sum()
            )
            self.insert(df, "eco", eco_analysis_col, None)

        logger.info("lyr")
        lyr_layer_ids = list(lyr["layer"].unique())
        for layer_id in lyr_layer_ids:
            for lyr_analysis_col in _lyr_analysis_cols:
                df = (
                    lyr.loc[lyr.layer == layer_id][
                        [lyr_analysis_col, "casfri_area"]
                    ]
                    .groupby(lyr_analysis_col)
                    .sum()
                )
                self.insert(df, "lyr", lyr_analysis_col, layer_id)

        logger.info("nfl")
        nfl_layer_ids = list(nfl["layer"].unique())
        for layer_id in nfl_layer_ids:
            for nfl_analysis_col in _nfl_analysis_cols:
                df = (
                    nfl.loc[nfl.layer == layer_id][
                        [nfl_analysis_col, "casfri_area"]
                    ]
                    .groupby(nfl_analysis_col)
                    .sum()
                )
                self.insert(df, "nfl", nfl_analysis_col, layer_id)

        logger.info("dst")
        dst_layer_ids = list(dst["layer"].unique())
        for layer_id in dst_layer_ids:
            for dst_analysis_col in _dst_analysis_cols:
                df = (
                    dst.loc[dst.layer == layer_id][
                        [dst_analysis_col, "casfri_area"]
                    ]
                    .groupby(dst_analysis_col)
                    .sum()
                )
                self.insert(df, "dst", dst_analysis_col, layer_id)

    def save_summary_tables(self, output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        for table in self.get_tables():
            for layer in self.get_layers(table):
                summary_data = self.get_summary_data(
                    table, layer, cleaned=True
                )
                for key, df in summary_data.items():
                    df.to_csv(os.path.join(output_dir, f"{key}.csv"))


def load_summary(data_dir: str) -> Summary:
    data = casfri_data.load_parquet(data_dir)
    for name in ["lyr", "eco", "nfl", "dst"]:
        data[name] = _merge_area(data[name], data["cas"])
    return Summary(data)


def display_summary(inventory_id: str, summary: Summary) -> None:
    display(Markdown(f"# {inventory_id}"))

    display(Markdown(f"## {inventory_id} hdr summary"))
    display(summary.get_raw_table("hdr").transpose())

    for table in summary.get_tables():
        display(Markdown(f"## {table} summary"))
        for layer in summary.get_layers(table):
            if layer is not None:
                display(Markdown(f"### {table} summary: layer {layer}"))
            summary_data = summary.get_summary_data(table, layer)
            for key, df in summary_data.items():
                display(Markdown(f"#### {key}"))
                if df.index.is_numeric() and len(df.index) > 1:
                    df.plot(marker="o", linestyle="none", figsize=(15, 5))
                else:
                    df.plot(kind="bar", figsize=(15, 5))
                plt.show()
                plt.close("all")
            display(
                Markdown(
                    f"#### {table} layer: {layer} null and "
                    "undefined value summary"
                )
            )
            display(summary.get_null_summary(table, layer))
