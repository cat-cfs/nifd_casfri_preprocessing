SELECT eco_all.* FROM eco_all
inner join cas_all on cas_all.cas_id = eco_all.cas_id
where cas_all.inventory_id = '{inventory_id}'