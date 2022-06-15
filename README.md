# nifd_casfri_preprocessing

## extract an inventory as a parquet dataset

```
nifd_casfri_extract --output_format parquet --host localhost --port 6666 --database nifd --username username --password password --inventory_id PE01 --output_dir ./casfri_data/PE01
```

## Create a summary and make copies of the raw casfri tables


postgres

```
nifd_casfri_summary --inventory_id_list PE01 --raw_table_dir \raw_tables --report_output_dir \report_output_dir
```

geopackage

```
nifd_casfri_summary --inventory_id_list PE01 --raw_table_dir \raw_tables --report_output_dir \report_output_dir
```