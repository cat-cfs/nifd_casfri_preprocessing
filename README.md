# nifd_casfri_preprocessing


## Create a summary and make copies of the raw casfri tables


postgres

```
nifd_casfri_summary --url "postgresql://user:pass@localhost:6666/nifd" --database_type 0 --inventory_id_list PE01 --raw_table_dir \raw_tables --report_output_dir \report_output_dir
```

geopackage

```
nifd_casfri_summary --url sqlite:///casfri_PE01.gpkg --database_type 1 --inventory_id_list PE01 --raw_table_dir \raw_tables --report_output_dir \report_output_dir
```

## extract an inventory as a geopackage

```

```