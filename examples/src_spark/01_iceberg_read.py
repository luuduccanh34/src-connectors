from src_connectors.src_spark import SparkConnector
import os

# --- Step 1: Initialize the Connector ---
# By default, it loads settings from environment variables (SparkVariables)
connector = SparkConnector()

# --- Step 2: Configure Iceberg ---
# We use the default environment settings for Iceberg,
# but we could also pass a dictionary or SparkIcebergConfig object here.
connector.configure_iceberg()

# --- Step 3: Connect and get the SparkSession ---
# You can overwrite any config at the last moment here if needed.
spark = connector.connect(spark_app_name="IcebergReadExample")

# --- Step 4: Execute Query using Spark SQL ---
try:
    # Warehouse name is used as the catalog identifier
    warehouse_name = os.getenv("SPARK_ICEBERG_WAREHOUSE", "lnd_etc")

    # Update this with a valid table in your Iceberg catalog
    query = f"SELECT * FROM {warehouse_name}.default.sample_table LIMIT 10"

    print(f"Executing query: {query}")
    df = connector.execute_query(query, output_type="dataframe")

    # Display results
    if df is not None:
        df.show()

finally:
    # --- Step 5: Close resources ---
    connector.close()
