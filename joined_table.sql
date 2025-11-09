-- Only ABN JOIN  works 
WITH cte_cc AS (
  SELECT abn_bigint AS abn_c, 
         REGEXP_REPLACE(url_host_registered_domain, '\.com\.au$|\.au$', '') AS url_name, 
         company_name, 
         business_info
  FROM (
    SELECT 
      cast (cc_abn AS BIGINT) AS abn_bigint,
      url_host_registered_domain,
      company_name,
      business_info,
      ROW_NUMBER() OVER (
        PARTITION BY REGEXP_REPLACE(url_host_registered_domain, '\.com\.au$|\.au$', '')  -- grouping by url_name
        ORDER BY 
          CASE WHEN cc_abn IS NOT NULL THEN 0 ELSE 1 END  -- prioritize records with ABN (0 first)
      ) AS rn
    FROM public.cc_table
    WHERE (cc_abn IS NOT NULL OR company_name IS NOT NULL)
  ) sub
  WHERE rn = 1
), cte_main as (
SELECT 
  a.abn, 
  a.abn_start_date, 
  a."entity_state", 
  a.entity_name, 
  b.abn_c, 
  b.company_name, 
  b.url_name, 
  b.business_info 
FROM abr_table a
JOIN cte_cc b
  ON a.abn IS NOT NULL 
     AND b.abn_c IS NOT NULL 
     AND a.abn = b.abn_c
 )
 select * from cte_main;



----- cleaned
 

WITH cte_cc AS (
  SELECT cast (cc_abn AS BIGINT) AS abn_bigint,
         company_name, 
         url_host_registered_domain,
         business_info
  FROM  cc_table 
  ), cte_main as (
SELECT 
  a.abn, 
  a.abn_start_date, 
  a."entity_state", 
  a.entity_name, 
  b.abn_bigint, 
  b.company_name, 
  b.url_host_registered_domain, 
  b.business_info 
FROM abr_table a
JOIN cte_cc b
  ON a.abn IS NOT NULL 
     AND b.abn_bigint IS NOT NULL 
     AND a.abn = b.abn_bigint
 )
 select * from cte_main;