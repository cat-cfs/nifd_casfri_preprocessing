SELECT geo_all.geometry, ROW_NUMBER() OVER (ORDER BY geo_all.cas_id) AS raster_id FROM geo_all inner join cas_all on cas_all.cas_id = geo_all.cas_id where cas_all.inventory_id = '{inventory_id}'