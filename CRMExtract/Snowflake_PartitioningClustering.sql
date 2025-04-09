CREATE TABLE customer_dimension (
    customer_id INT,
    customer_name STRING,
    customer_email STRING,
    customer_phone STRING
)
CLUSTER BY (customer_id);
