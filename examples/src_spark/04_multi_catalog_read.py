from src_connectors.src_spark import SparkConnector

# --- Step 1: Initialize the Connector ---
connector = SparkConnector()

# --- Step 2: Configure Multiple Iceberg Catalogs ---
# We can now pass a list of iceberg_warehouse names.
# Each name in the list will be used as a catalog prefix.
connector.configure_iceberg({
    "iceberg_warehouse": ["prod_catalog", "staging_catalog"],
    "iceberg_catalog_type": "rest",  # Both will share this type in this simplified example
    "iceberg_catalog_uri": "/tmp/iceberg_warehouse" # Base path for hadoop catalog
})

# --- Step 3: Connect and get the SparkSession ---
spark = connector.connect(spark_app_name="MultiCatalogExample")

# --- Step 4: Verify Configuration ---
try:
    print("Checking Spark configurations for multiple catalogs:")

    # Check if both catalogs are configured in Spark
    prod_type = spark.conf.get("spark.sql.catalog.prod_catalog", None)
    staging_type = spark.conf.get("spark.sql.catalog.staging_catalog", None)

    print(f"prod_catalog implementation: {prod_type}")
    print(f"staging_catalog implementation: {staging_type}")

    # Check warehouse locations
    prod_wh = spark.conf.get("spark.sql.catalog.prod_catalog.warehouse", None)
    staging_wh = spark.conf.get("spark.sql.catalog.staging_catalog.warehouse", None)

    print(f"prod_catalog warehouse: {prod_wh}")
    print(f"staging_catalog warehouse: {staging_wh}")

    print("\nNote: In a real scenario, you could now perform cross-catalog queries like:")
    print("SELECT * FROM prod_catalog.db.table JOIN staging_catalog.db.table ON ...")

finally:
    # --- Step 5: Close resources ---
    connector.close()
