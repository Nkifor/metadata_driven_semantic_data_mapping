SELECT 
    'crm_customers_raw' AS table_name, 
    COUNT(*) AS row_count
FROM mapping_demo_dev.bronze.crm_customers_raw

UNION ALL

SELECT 
    'erp_clients_raw' AS table_name, 
    COUNT(*) AS row_count
FROM mapping_demo_dev.bronze.erp_clients_raw