# nifd_casfri_preprocessing

Requirements
* python 3.9+
* gdal command line tools

## extract an inventory as a parquet dataset

Extracts information associated with the specified inventory into parquet tables along with a rasterized version of cas_id

```
nifd_casfri_extract --output_format parquet --host localhost --port 6666 --database nifd --username username --password password --inventory_id PE01 --output_dir ./casfri_data/PE01
```

## Create a data summary of parquet dataset

Generates a jupyter notebook/html output exploring area distributions of defined values and extent of null or unddefined values

```
nifd_casfri_summary --inventory_id PE01 --raw_table_dir \raw_tables\PE01 --report_output_dir \report_output_dir
```
