from src_connectors.src_spark import SparkConnector

# --- Example: Overwriting configs during initialization ---
# We can pass raw spark properties directly in the config dictionary.
# Underscores are automatically converted to dots: spark_master -> spark.master
connector = SparkConnector(
    spark_master="local[2]",
    spark_executor_memory="1g"
)

# --- Overwriting Iceberg specific fields via dictionary ---
connector.configure_iceberg({
    "iceberg_catalog_type": "rest",
    "iceberg_warehouse": "my_custom_catalog"
})

try:
    # --- Overwriting at the moment of connection ---
    # These have the highest priority.
    spark = connector.connect(spark_app_name="OverrideApp")

    print("Spark context initialized with custom overrides.")
    print(f"Master: {spark.conf.get('spark.master')}")
    print(f"App Name: {spark.conf.get('spark.app.name')}")

finally:
    connector.close()
