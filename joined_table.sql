SELECT 
    t.abn, c.cc_abn, t.clean_entity_name, c.company_name,
    t.abn_start_date, c.url, c.meta_info, c.business_info
FROM abr_table t
INNER JOIN cc_table c ON t.abn = c.cc_abn
UNION
SELECT 
    t.abn, c.cc_abn, t.clean_entity_name, c.company_name,
    t.abn_start_date, c.url, c.meta_info, c.business_info
FROM abr_table t
INNER JOIN cc_table c ON t.clean_entity_name = c.company_name
