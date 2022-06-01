SELECT lyr_all.* FROM lyr_all
inner join cas_all on cas_all.cas_id = lyr_all.cas_id
where cas_all.inventory_id = '{inventory_id}'