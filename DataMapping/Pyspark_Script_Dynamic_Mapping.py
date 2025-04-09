from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import boto3
import psycopg2
import logging

# --------------------------------------
# 1. Spark Session and AWS Config
# --------------------------------------
spark = SparkSession.builder \
    .appName("Redshift Dynamic Mapping") \
    .config("spark.jars", "/path/to/redshift-jdbc42.jar") \
    .getOrCreate()

# AWS S3 Bucket and Redshift connection info
S3_BUCKET = "s3-bucket"
S3_KEY = "input_file.csv"
REDSHIFT_HOST = "redshift-cluster-name.region.redshift.amazonaws.com"
REDSHIFT_PORT = "5439"
REDSHIFT_DB = "database"
REDSHIFT_USER = "user"
REDSHIFT_PASSWORD = "your_password"
REDSHIFT_TABLE = "public.state_data"
LOG_TABLE = "public.ingestion_log"

# --------------------------------------
# 2. Define Column Mappings
# --------------------------------------
MAPPINGS = {
    "state": ["state", "customer_state", "user_state", "st", "region"],
    "city": ["city", "town", "municipality"],
    "zipcode": ["zip", "postal_code", "zipcode"]
}

# --------------------------------------
# 3. Read Flat File from S3
# --------------------------------------
s3_client = boto3.client('s3')
file_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
file_content = file_obj['Body'].read().decode('utf-8')

# Load file into PySpark DataFrame
df = spark.read.csv(f"s3://{S3_BUCKET}/{S3_KEY}", header=True, inferSchema=True)

# --------------------------------------
# 4. Dynamic Column Renaming
# --------------------------------------
def dynamic_mapping(df, mappings):
    log_entries = []

    # Identify and rename columns
    for standard_col, variations in mappings.items():
        matched_cols = [col for col in df.columns if col.lower() in variations]
        
        if matched_cols:
            for col_name in matched_cols:
                # Rename the column to the standard name
                df = df.withColumnRenamed(col_name, standard_col)

                # Add mapping log entry
                log_entries.append((S3_KEY, col_name, standard_col, "Mapped", ""))
        else:
            log_entries.append((S3_KEY, "", standard_col, "Missing", "No matching column found"))

    # Add missing standard columns if not in the file
    for std_col in mappings.keys():
        if std_col not in df.columns:
            df = df.withColumn(std_col, lit(None))

    return df, log_entries

# Apply the mapping
df, logs = dynamic_mapping(df, MAPPINGS)

# --------------------------------------
# 5. Write Data to Redshift
# --------------------------------------
# Save DataFrame to Redshift
df.write \
    .format("jdbc") \
    .option("url", f"jdbc:redshift://{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}") \
    .option("dbtable", REDSHIFT_TABLE) \
    .option("user", REDSHIFT_USER) \
    .option("password", REDSHIFT_PASSWORD) \
    .mode("append") \
    .save()

# --------------------------------------
# 6. Write Logs to Redshift
# --------------------------------------
def write_logs_to_redshift(logs):
    """Write mapping logs to Redshift."""
    conn = psycopg2.connect(
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT
    )
    cur = conn.cursor()

    for log in logs:
        cur.execute(
            f"""
            INSERT INTO {LOG_TABLE} (source_file, column_name, mapped_to, status, error_message)
            VALUES (%s, %s, %s, %s, %s)
            """,
            log
        )
    conn.commit()
    cur.close()
    conn.close()

# Write logs to Redshift
write_logs_to_redshift(logs)

# --------------------------------------
# 7. Clean Up and Logging
# --------------------------------------
logging.info("Data loaded successfully into Redshift")
spark.stop()
