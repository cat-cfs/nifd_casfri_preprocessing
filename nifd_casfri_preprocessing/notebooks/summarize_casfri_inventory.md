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
import matplotlib.pyplot as plt
```

```python
# sqlite
path = r"C:\Users\scott\dev\projects\2022_wall_to_wall\casfri_PE01.gpkg"
url = f"sqlite:///{path}"
```

```python
# postgres
# url = "postgresql://casfri:casfri@localhost:6666/nifd"
```

```python
engine = sa.create_engine(url)
inspector = sa.inspect(engine)
table_names = inspector.get_table_names()
```

```python
table_names
```

```python

```

```python
inventory_id = "PE01"
cas = pd.read_sql("SELECT * from cas", engine)
lyr = pd.read_sql("SELECT * from lyr", engine).merge(cas[["cas_id", "casfri_area"]])
eco = pd.read_sql("SELECT * from eco", engine).merge(cas[["cas_id", "casfri_area"]])
hdr = pd.read_sql("SELECT * from hdr", engine)
nfl = pd.read_sql("SELECT * from nfl", engine).merge(cas[["cas_id", "casfri_area"]])
dst = pd.read_sql("SELECT * from dst", engine).merge(cas[["cas_id", "casfri_area"]])
```

```python
hdr.transpose()
```

```python
cas_analysis_cols = ["stand_structure", "num_of_layers", "stand_photo_year"]
lyr_analysis_cols = [
    'soil_moist_reg', 'structure_per', 'structure_range',
       'crown_closure_upper', 'crown_closure_lower',
       'height_upper', 'height_lower', 'productivity', 'productivity_type',
       'origin_upper', 'origin_lower', 'site_class', 'site_index']
lyr_species_components_cols = [
   'species_1', 'species_per_1', 'species_2', 'species_per_2', 'species_3',
   'species_per_3', 'species_4', 'species_per_4', 'species_5',
   'species_per_5', 'species_6', 'species_per_6', 'species_7',
   'species_per_7', 'species_8', 'species_per_8', 'species_9',
   'species_per_9', 'species_10', 'species_per_10']
eco_analysis_cols = [
    'wetland_type', 'wet_veg_cover', 'wet_landform_mod',
    'wet_local_mod', 'eco_site']
nfl_analysis_cols = [
    'soil_moist_reg', 'structure_per', 'crown_closure_upper', 'crown_closure_lower',
    'height_upper', 'height_lower', 'nat_non_veg', 'non_for_anth',
    'non_for_veg'
]
dst_analysis_cols = [
    'dist_type_1', 'dist_year_1', 'dist_ext_upper_1',
       'dist_ext_lower_1', 'dist_type_2', 'dist_year_2', 'dist_ext_upper_2',
       'dist_ext_lower_2', 'dist_type_3', 'dist_year_3', 'dist_ext_upper_3',
       'dist_ext_lower_3'
]
```

```python
analysis = {
    "cas": {},
    "eco": {},
    "lyr": {},
    "lyr_species": {},
    "nfl": {},
    "dst": {}
}
for cas_analysis_col in cas_analysis_cols:
    analysis["cas"][cas_analysis_col] = cas[[cas_analysis_col, "casfri_area"]].groupby(cas_analysis_col).sum()

for eco_analysis_col in eco_analysis_cols:
    analysis["eco"][eco_analysis_col] = eco[[eco_analysis_col, "casfri_area"]].groupby(eco_analysis_col).sum()
    
lyr_layer_ids = list(lyr["layer"].unique())
for layer_id in lyr_layer_ids:
    data = {}
    for lyr_analysis_col in lyr_analysis_cols:
        data[lyr_analysis_col] = lyr.loc[lyr.layer==layer_id][[lyr_analysis_col, "casfri_area"]].groupby(lyr_analysis_col).sum()
    analysis["lyr"][layer_id] = data 
    
lyr_layer_ids = list(lyr["layer"].unique())
for layer_id in lyr_layer_ids:
    data = {}
    for lyr_analysis_col in lyr_species_components_cols:
        data[lyr_analysis_col] = lyr.loc[lyr.layer==layer_id][[lyr_analysis_col, "casfri_area"]].groupby(lyr_analysis_col).sum()
    analysis["lyr_species"][layer_id] = data 
    
nfl_layer_ids = list(nfl["layer"].unique())
for layer_id in nfl_layer_ids:
    
    data = {}
    for nfl_analysis_col in nfl_analysis_cols:
        data[nfl_analysis_col] = nfl.loc[nfl.layer==layer_id][[nfl_analysis_col, "casfri_area"]].groupby(nfl_analysis_col).sum()
    analysis["nfl"][layer_id] = data 
    
dst_layer_ids = list(dst["layer"].unique())
for layer_id in dst_layer_ids:
    
    data = {}
    for dst_analysis_col in dst_analysis_cols:
        data[dst_analysis_col] = dst.loc[dst.layer==layer_id][[dst_analysis_col, "casfri_area"]].groupby(dst_analysis_col).sum()
    analysis["dst"][layer_id] = data 
```

```python
for k in analysis["dst"].keys():
    fig, axes = plt.subplots(nrows=3, ncols=4, figsize=(15,15))
    for idx, (name, df) in enumerate(analysis["dst"][k].items()):        
        df.plot(ax=axes[idx//4, idx%4], kind="bar")
        fig.suptitle(f'{inventory_id} casfri dst (layer id {k})', fontsize=16)
        plt.tight_layout()

```

```python
for k in analysis["nfl"].keys():
    fig, axes = plt.subplots(nrows=3, ncols=3, figsize=(15,15))
    for idx, (name, df) in enumerate(analysis["nfl"][k].items()):        
        df.plot(ax=axes[idx//3, idx%3], kind="bar")
        fig.suptitle(f'{inventory_id} casfri nfl table summary (layer id {k})', fontsize=16)
        plt.tight_layout()
```

```python
for k in analysis["lyr"].keys():
    fig, axes = plt.subplots(nrows=5, ncols=3, figsize=(15,15))
    for idx, (name, df) in enumerate(analysis["lyr"][k].items()):        
        df.plot(ax=axes[idx//3, idx%3], kind="bar")
        fig.suptitle(f'{inventory_id} casfri lyr table summary (layer id {k})', fontsize=16)
        plt.tight_layout()
```

```python
for k in analysis["lyr_species"].keys():
    fig, axes = plt.subplots(nrows=10, ncols=2, figsize=(15,40))
    for idx, (name, df) in enumerate(analysis["lyr_species"][k].items()):        
        df.plot(ax=axes[idx//2, idx%2], kind="bar")
        fig.suptitle(f'{inventory_id} casfri lyr (species) table summary (layer id {k})', fontsize=16)
        plt.tight_layout()
```

```python
fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(10,15))
for idx, (name, df) in enumerate(analysis["eco"].items()):        
    df.plot(ax=axes[idx//2, idx%2], kind="bar")
    fig.suptitle(f'{inventory_id} casfri eco table summary', fontsize=16)
    plt.tight_layout()
```

```python
fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(10,15))
for idx, (name, df) in enumerate(analysis["cas"].items()):        
    df.plot(ax=axes[idx%3], kind="bar")
    fig.suptitle(f'{inventory_id} casfri cas table summary', fontsize=16)
    plt.tight_layout()

```

```python

```
