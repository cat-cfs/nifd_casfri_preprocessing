# nifd_casfri_preprocessing

Requirements
* python 3.9+
* gdal command line tools

## extract an inventory as a parquet dataset

Extracts information associated with the specified inventory into parquet tables along with a rasterized version of cas_id.  This produces a replica of the original inventory, however the spatial geometry is rasterized using the specified resolution

```
nifd_casfri_extract --resolution 100 --output_format parquet --host localhost --port 6666 --database nifd --username username --password password --inventory_id PE01 --output_dir ./casfri_data/PE01
```

## Create a data summary of parquet dataset

Generates a jupyter notebook/html output exploring area distributions of defined values and extent of null or unddefined values

```
nifd_casfri_summary --inventory_id PE01 --raw_table_dir ./casfri_data/PE01 --report_output_dir \report_output_dir
```

## Extract selected rasterized CBM inputs

Extract/preprocess rasterized variables/attribute tables geared to CBM

```
nifd_casfri_process --data_dir ./casfri_data/PE01 --out_dir ./processed/PE01 --wgs84 --age_relative_year 2022
```
