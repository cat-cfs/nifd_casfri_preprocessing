import os
import pandas as pd
from nifd_casfri_preprocessing.gis_helpers import gdal_helpers
from nifd_casfri_preprocessing import casfri_data


class ParquetGeoDataset:
    def __init__(self, data_dir, inventory_id, wgs84=True):
        self.data_dict: dict[str, pd.DataFrame] = casfri_data.load_parquet(
            data_dir
        )
        raster_filename = "cas_id_wgs84.tiff" if wgs84 else "cas_id.tiff"
        base_raster_path = os.path.join(data_dir, raster_filename)
        self.raster = gdal_helpers.read_dataset(base_raster_path)

    @property
    def raster(self) -> gdal_helpers.GDALHelperDataset:
        return self.raster

    @property
    def hdr(self) -> pd.DataFrame:
        return self.data_dict["hdr"]

    @property
    def cas(self) -> pd.DataFrame:
        return self.data_dict["cas"]

    @property
    def eco(self) -> pd.DataFrame:
        return self.data_dict["eco"]

    @property
    def lyr(self) -> pd.DataFrame:
        return self.data_dict["lyr"]

    @property
    def nfl(self) -> pd.DataFrame:
        return self.data_dict["nfl"]

    @property
    def dst(self) -> pd.DataFrame:
        return self.data_dict["dst"]

    @property
    def geo_lookup(self) -> pd.DataFrame:
        return self.data_dict["geo_lookup"]


def process_species_components(ds: ParquetGeoDataset, out_dir: str) -> None:

    species_cols = []
    for x in range(1, 11):
        species_cols.extend([f"species_{x}", f"species_per_{x}"])
    species_view = ds.lyr[ds.lyr.layer == 1][["cas_id"] + species_cols].copy()

    keep_cols = species_cols.copy()
    # drop from the above species cols where nothing is defined
    for col in species_cols:

        col_data = species_view[col]
        col_data_unique = col_data.unique()
        if col_data_unique.shape[0] == 1:
            if col_data_unique[0] in [
                "NULL_VALUE",
                "NOT_APPLICABLE",
                -8888,
                -8887,
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
        out_dir, "species_composition_1.tiff"
    )
    out_species_composition_att_path = os.path.join(
        out_dir, "species_composition_1.csv"
    )
    gdal_helpers.create_empty_raster(
        base_raster_path,
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
