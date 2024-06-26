---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.13.7
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

```python
import os
import sqlalchemy as sa
import pandas as pd
```

```python
out_dir = r"T:\39_wall_to_wall\05_working\00_preprocessing\schema_browse"
if not os.path.exists(out_dir):
    os.makedirs(out_dir)
```

```python
url = "postgresql://casfri:casfri@localhost:6666/nifd"
```

```python
engine = sa.create_engine(url)
```

```python
inspector = sa.inspect(engine)
```

```python
table_names = inspector.get_table_names()
```

```python
table_names
```

```python
with pd.ExcelWriter(os.path.join(out_dir,"data_sample.xlsx")) as writer:
    for table in table_names:
        print(table)
        pd.read_sql(
            f"SELECT * from {table} limit 10", engine
        ).to_excel(writer, sheet_name=table, index=False)
```

```python
pd.read_sql("""
SELECT
geo_all.cas_id, 
ROW_NUMBER() OVER (ORDER BY geo_all.cas_id) AS raster_id
FROM geo_all
inner join cas_all on cas_all.cas_id = geo_all.cas_id
where cas_all.inventory_id = 'PE01'
limit 10
""", engine)
# GROUP BY cas_id;
```

```python

```

<!-- #region -->
rasterization command
```powershell
gdal_rasterize -a raster_id -tr 30.0 30.0 PG:host="localhost dbname=nifd port=6666 user=casfri password=casfri" -sql "SELECT geo_all.geometry, ROW_NUMBER() OVER (ORDER BY geo_all.cas_id) AS raster_id FROM geo_all inner join cas_all on cas_all.cas_id = geo_all.cas_id where cas_all.inventory_id = 'PE01'" -co COMPRESS=DEFLATE -co BIGTIFF=YES -ot Int64 -a_nodata -1 PEI_CAS_ID.tiff
```
<!-- #endregion -->

```python
pd.read_sql("SELECT * from cas_all limit 10", engine)
```

```python
pd.read_sql("SELECT count(lyr_all.cas_id) from lyr_all", engine)
```

```python
pd.read_sql("SELECT pg_size_pretty(pg_database_size('nifd'))", engine)
```

```python
pd.read_sql("SELECT * from hdr_all", engine).to_excel(os.path.join(out_dir,"hdr_all.xlsx"), index=False)
```

```python
relationships = {}
for table in table_names:

    relationships[table] = inspector.get_foreign_keys(table)
```

```python
relationships
```

```python
cas_inventory_ids = pd.read_sql(
    f"SELECT distinct(inventory_id) from cas_all", engine
)
```

```python
hdr_all = pd.read_sql("SELECT * from hdr_all", engine)
```

```python
cas_inventory_ids.inventory_id.to_list()
```

```python

```
