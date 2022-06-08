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
inventory_id = "PE01"
raw_data_path = r"C:\Users\scott\dev\projects\2022_wall_to_wall\raw_tables"
```


```python
from nifd_casfri_preprocessing import data_summary
```


```python
summary = data_summary.load_summary(raw_data_path)
```


```python
data_summary.display_summary(inventory_id, summary)
```

```python

```
