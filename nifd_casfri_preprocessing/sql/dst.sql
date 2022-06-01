SELECT dst_all.* FROM dst_all
inner join cas_all on cas_all.cas_id = dst_all.cas_id
where cas_all.inventory_id = '{inventory_id}'