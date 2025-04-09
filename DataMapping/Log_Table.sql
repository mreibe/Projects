CREATE TABLE public.ingestion_log (
    id INT IDENTITY(1,1),
    source_file VARCHAR(255),
    column_name VARCHAR(255),
    mapped_to VARCHAR(255),
    status VARCHAR(50),
    error_message VARCHAR(255),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
