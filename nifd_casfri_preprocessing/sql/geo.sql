SELECT * FROM geo_all
inner join cas_all on cas_all.cas_id = geo_all.cas_id
where cas_all.inventory_id = {inventory_id}