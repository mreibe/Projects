-- Deduplication transformation using DBT
WITH deduplicated_data AS (
    SELECT 
        customer_id,
        customer_name,
        customer_email,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS row_num
    FROM raw_crm_data
)
SELECT 
    customer_id, 
    customer_name, 
    customer_email
FROM deduplicated_data
WHERE row_num = 1;