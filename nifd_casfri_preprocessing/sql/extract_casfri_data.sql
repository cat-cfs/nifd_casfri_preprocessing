SELECT 
geo_all.geometry, dst_all.* from dst_all
inner join cas_all on dst_all.cas_id = cas_all.cas_id
inner join hdr_all on cas_all.inventory_id = hdr_all.inventory_id
inner join geo_all on geo_all.cas_id = cas_all.cas_id
where hdr_all.jurisdiction = 'Prince Edward Island'