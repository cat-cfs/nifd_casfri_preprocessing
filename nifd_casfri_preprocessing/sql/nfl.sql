SELECT nfl_all.* FROM nfl_all
inner join cas_all on cas_all.cas_id = nfl_all.cas_id
where cas_all.inventory_id = '{inventory_id}'