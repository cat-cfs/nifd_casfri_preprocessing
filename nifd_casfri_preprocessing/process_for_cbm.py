import os
import numpy as np
import pandas as pd
from nifd_casfri_preprocessing.gis_helpers import gdal_helpers
from nifd_casfri_preprocessing import casfri_data
from nifd_casfri_preprocessing import log_helper

logger = log_helper.get_logger()


class ParquetGeoDataset:
    def __init__(self, data_dir: str, wgs84: bool):
        self._data_dict: dict[str, pd.DataFrame] = casfri_data.load_parquet(
            data_dir
        )
        raster_filename = "cas_id_wgs84.tiff" if wgs84 else "cas_id.tiff"
        self._base_raster_path = os.path.join(data_dir, raster_filename)
        self._raster = gdal_helpers.read_dataset(self.base_raster_path)

    @property
    def base_raster_path(self) -> str:
        return self._base_raster_path

    @property
    def raster(self) -> gdal_helpers.GDALHelperDataset:
        return self._raster

    @property
    def hdr(self) -> pd.DataFrame:
        return self._data_dict["hdr"]

    @property
    def cas(self) -> pd.DataFrame:
        return self._data_dict["cas"]

    @property
    def eco(self) -> pd.DataFrame:
        return self._data_dict["eco"]

    @property
    def lyr(self) -> pd.DataFrame:
        return self._data_dict["lyr"]

    @property
    def nfl(self) -> pd.DataFrame:
        return self._data_dict["nfl"]

    @property
    def dst(self) -> pd.DataFrame:
        return self._data_dict["dst"]

    @property
    def geo_lookup(self) -> pd.DataFrame:
        return self._data_dict["geo_lookup"]


def get_layer_subdir(layer_id: int) -> str:
    return f"layer_{layer_id}"


def create_layer_index(ds: ParquetGeoDataset, out_dir: str) -> pd.DataFrame:
    lyr_layer_ids = set(list(ds.lyr["layer"].unique()))
    dst_layer_ids = set(list(ds.dst["layer"].unique()))
    all_layer_ids = lyr_layer_ids.union(dst_layer_ids)
    layer_index = []
    for _id in all_layer_ids:
        subdir = get_layer_subdir(_id)
        if not os.path.exists(os.path.join(out_dir, subdir)):
            os.makedirs(os.path.join(out_dir, subdir))
        layer_index.append(
            (
                _id,
                subdir,
                _id in lyr_layer_ids,
                _id in dst_layer_ids,
            )
        )
    df = pd.DataFrame(
        columns=[
            "casfri_layer_id",
            "subdir",
            "defined_in_lyr",
            "defined_in_dst",
        ],
        data=layer_index,
    )
    df.to_csv(os.path.join(out_dir, "layer_index.csv"), index=False)
    return df


def process_origin(
    layer_id: int, ds: ParquetGeoDataset, out_dir: str, age_relative_year: int
) -> None:
    mean_origin_view = ds.lyr.loc[ds.lyr["layer"] == layer_id][
        ["cas_id", "origin_upper", "origin_lower"]
    ].copy()
    mean_origin_view = ds.geo_lookup.merge(
        mean_origin_view, left_on="cas_id", right_on="cas_id"
    )
    # filter out undefined casfri values
    mean_origin_view = mean_origin_view[
        (mean_origin_view["origin_upper"] > 0)
        & (mean_origin_view["origin_lower"] > 0)
    ].copy()

    mean_origin_view["mean_origin"] = (
        (
            (
                mean_origin_view["origin_upper"]
                + mean_origin_view["origin_lower"]
            )
            / 2
        )
        .round()
        .astype("int")
    )

    out_data = pd.DataFrame(
        columns=["raster_id"], data=ds.raster.data.flatten()
    )
    out_data = out_data.merge(
        mean_origin_view[["raster_id", "mean_origin"]], how="left"
    )
    out_data_np = out_data["mean_origin"].fillna(-1).to_numpy()
    out_mean_origin_path = os.path.join(out_dir, "mean_origin.tiff")
    gdal_helpers.create_empty_raster(
        ds.base_raster_path,
        out_mean_origin_path,
        data_type=np.int32,
        nodata=-1,
        options=gdal_helpers.get_default_geotiff_creation_options(),
    )
    gdal_helpers.write_output(
        out_mean_origin_path,
        out_data_np.reshape(ds.raster.data.shape),
        x_off=0,
        y_off=0,
    )

    out_age_path = os.path.join(out_dir, f"age_{age_relative_year}.tiff")
    gdal_helpers.create_empty_raster(
        ds.base_raster_path,
        out_age_path,
        data_type=np.int32,
        nodata=-1,
        options=gdal_helpers.get_default_geotiff_creation_options(),
    )
    out_age_data = out_data_np.copy()
    out_age_data[out_age_data > 0] = (
        age_relative_year - out_age_data[out_age_data > 0]
    )
    gdal_helpers.write_output(
        out_age_path,
        out_age_data.reshape(ds.raster.data.shape),
        x_off=0,
        y_off=0,
    )


def process_leading_species(
    layer_id: int, ds: ParquetGeoDataset, out_dir: str
) -> None:
    leading_species_view = ds.lyr[ds.lyr.layer == 1][
        ["cas_id", "species_1"]
    ].copy()
    leading_species_view = ds.geo_lookup.merge(leading_species_view)
    leading_species_view_unique = (
        leading_species_view[["species_1"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    leading_species_view_unique.index += 1
    leading_species_view_unique.index.name = "species_id"
    leading_species_view_unique = leading_species_view_unique.reset_index()
    leading_species_view = leading_species_view.merge(
        leading_species_view_unique
    )
    leading_species_raster_data = (
        pd.DataFrame({"raster_id": ds.raster.data.flatten()})
        .merge(leading_species_view[["raster_id", "species_id"]], how="left")
        .fillna(-1)["species_id"]
        .astype("int32")
        .to_numpy()
        .reshape(ds.raster.data.shape)
    )

    out_leading_species_path = os.path.join(out_dir, "leading_species.tiff")
    out_leading_att_path = os.path.join(out_dir, "leading_species.csv")
    gdal_helpers.create_empty_raster(
        ds.base_raster_path,
        out_leading_species_path,
        data_type=np.int32,
        nodata=-1,
        options=gdal_helpers.get_default_geotiff_creation_options(),
    )
    gdal_helpers.write_output(
        out_leading_species_path, leading_species_raster_data, x_off=0, y_off=0
    )
    leading_species_view_unique.to_csv(
        out_leading_att_path,
        header=["raster_id", "casfri_species_name"],
        index=False,
    )


def process_disturbance_events(
    layer_id: int, ds: ParquetGeoDataset, out_dir: str
) -> None:

    for disturbance_col_num in range(1, 4):
        dist_view = ds.dst[ds.dst["layer"] == layer_id].copy()
        data_cols = [
            f"dist_type_{disturbance_col_num}",
            f"dist_year_{disturbance_col_num}",
            f"dist_ext_upper_{disturbance_col_num}",
            f"dist_ext_lower_{disturbance_col_num}",
        ]
        dist_view = dist_view[["cas_id"] + data_cols]
        dist_view_unique = dist_view.drop_duplicates(data_cols)[data_cols]
        dist_view_unique = dist_view_unique.reset_index(drop=True)
        dist_view_unique.index += 1
        dist_view_unique.index.name = "disturbance_id"

        dist_view_unique = dist_view_unique.reset_index()
        dist_view = dist_view.merge(
            dist_view_unique, left_on=data_cols, right_on=data_cols
        )
        dist_view = dist_view.merge(ds.geo_lookup)
        disturbance_raster_data = (
            pd.DataFrame({"raster_id": ds.raster.data.flatten()})
            .merge(dist_view[["raster_id", "disturbance_id"]], how="left")
            .fillna(-1)["disturbance_id"]
            .astype("int32")
            .to_numpy()
            .reshape(ds.raster.data.shape)
        )
        out_disturbances_path = os.path.join(
            out_dir, f"disturbances_{disturbance_col_num}.tiff"
        )
        out_disturbances_att_path = os.path.join(
            out_dir, f"disturbances_{disturbance_col_num}.csv"
        )
        gdal_helpers.create_empty_raster(
            ds.base_raster_path,
            out_disturbances_path,
            data_type=np.int32,
            nodata=-1,
            options=gdal_helpers.get_default_geotiff_creation_options(),
        )
        gdal_helpers.write_output(
            out_disturbances_path, disturbance_raster_data, x_off=0, y_off=0
        )

        dist_view_unique.to_csv(
            out_disturbances_att_path,
            header=[
                "raster_id",
                "dist_type",
                "dist_year",
                "dist_ext_upper",
                "dist_ext_lower",
            ],
            index=False,
        )


def process_species_components(
    layer_id: int, ds: ParquetGeoDataset, out_dir: str
) -> None:

    species_cols = []
    for x in range(1, 11):
        species_cols.extend([f"species_{x}", f"species_per_{x}"])

    species_view = ds.lyr[ds.lyr["layer"] == layer_id][
        ["cas_id"] + species_cols
    ].copy()

    keep_cols = species_cols.copy()
    # drop from the above species cols where nothing is defined
    for col in species_cols:

        col_data = species_view[col]
        col_data_unique = col_data.unique()
        if col_data_unique.shape[0] == 1:
            if str(col_data_unique[0]) in [
                "NULL_VALUE",
                "NOT_APPLICABLE",
                "UNKNOWN_VALUE",
                -8888,
                -8887,
                -8886,
            ]:
                keep_cols.remove(col)

    species_cols = keep_cols
    species_view = species_view[["cas_id"] + species_cols].copy()
    species_view_unique = species_view.drop_duplicates(species_cols)[
        species_cols
    ]
    species_view_unique = species_view_unique.reset_index(drop=True)
    species_view_unique.index += 1
    species_view_unique.index.name = "species_composition_id"
    species_view_unique = species_view_unique.reset_index()

    species_view = species_view.merge(species_view_unique)
    species_view = ds.geo_lookup.merge(species_view)
    species_composition_raster_data = (
        pd.DataFrame({"raster_id": ds.raster.data.flatten()})
        .merge(
            species_view[["raster_id", "species_composition_id"]], how="left"
        )
        .fillna(-1)["species_composition_id"]
        .astype("int32")
        .to_numpy()
        .reshape(ds.raster.data.shape)
    )
    out_species_composition_path = os.path.join(
        out_dir, "species_composition.tiff"
    )
    out_species_composition_att_path = os.path.join(
        out_dir, "species_composition.csv"
    )
    gdal_helpers.create_empty_raster(
        ds.base_raster_path,
        out_species_composition_path,
        data_type=np.int32,
        nodata=-1,
        options=gdal_helpers.get_default_geotiff_creation_options(),
    )
    gdal_helpers.write_output(
        out_species_composition_path,
        species_composition_raster_data,
        x_off=0,
        y_off=0,
    )

    species_view_unique.to_csv(
        out_species_composition_att_path,
        header=["raster_id"] + species_cols,
        index=False,
    )


def process(
    data_dir: str, wgs84: bool, age_relative_year: int, out_dir: str
) -> None:
    logger.info(f"loading dataset from {data_dir}")
    ds = ParquetGeoDataset(data_dir, wgs84)

    layer_index = create_layer_index(ds, out_dir)
    lyr_layer_ids = [
        int(x)
        for x in layer_index[layer_index["defined_in_lyr"]]["casfri_layer_id"]
    ]
    dst_layer_ids = [
        int(x)
        for x in layer_index[layer_index["defined_in_dst"]]["casfri_layer_id"]
    ]
    for layer_id in lyr_layer_ids:
        layer_subdir = os.path.join(out_dir, get_layer_subdir(layer_id))
        logger.info("process origin data")
        process_origin(layer_id, ds, layer_subdir, age_relative_year)
        logger.info("process leading species data")
        process_leading_species(layer_id, ds, layer_subdir)
        logger.info("process species components data")
        process_species_components(layer_id, ds, layer_subdir)
    for layer_id in dst_layer_ids:
        layer_subdir = os.path.join(out_dir, get_layer_subdir(layer_id))
        logger.info("process disturbance events data")
        process_disturbance_events(layer_id, ds, layer_subdir)
