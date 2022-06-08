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
# sqlite
path = r"C:\Users\scott\dev\projects\2022_wall_to_wall\casfri_PE01.gpkg"
url = f"sqlite:///{path}"
inventory_id="PE01"
database_type=1
```

```python
# postgres
# url = "postgresql://casfri:casfri@localhost:6666/nifd"
```

```python
from nifd_casfri_preprocessing import data_summary
```

```python
data = data_summary.load_data(url, database_type, inventory_id)
```

```python
summary = data_summary.Summary(data)
```

```python
summary.get_tables()
```

```python

```

```python
data_summary.display_summary(inventory_id, summary)
            
```

```python
dst = summary.get_raw_table("dst")
```

```python
dst[["cas_id", "casfri_area"]].drop_duplicates("cas_id")["casfri_area"].sum()
```

```python

```

```python

```
