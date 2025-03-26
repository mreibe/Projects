-- Create Stream on staging table
CREATE OR REPLACE STREAM crm_staging_stream ON TABLE crm_staging
    SHOW_INITIAL_ROWS = TRUE;

-- Create Task to load data into the final customer table
CREATE OR REPLACE TASK load_crm_data
    WAREHOUSE = my_warehouse
    SCHEDULE = 'USING CRON 0 0 * * * UTC'
AS
    MERGE INTO customer_dimension AS target
    USING crm_staging AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN UPDATE SET target.customer_name = source.customer_name
    WHEN NOT MATCHED THEN INSERT (customer_id, customer_name) VALUES (source.customer_id, source.customer_name);
