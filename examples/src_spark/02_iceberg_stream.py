from src_connectors.src_spark import SparkConnector
import os

# Initialize connector
connector = SparkConnector()

# Configure Iceberg
connector.configure_iceberg()

try:
    warehouse_name = os.getenv("SPARK_ICEBERG_WAREHOUSE", "local")
    query = f"SELECT * FROM {warehouse_name}.default.sample_table"

    print(f"Streaming results from: {query}")

    # Using stream=True returns a Generator yielding batches
    # output_type can be 'list', 'array', or 'dataframe' (Pandas)
    batches = connector.execute_query(
        query,
        stream=True,
        batch_size=100,
        output_type="list"
    )

    for i, batch in enumerate(batches):
        print(f"Batch {i+1} received. Row count: {len(batch)}")
        # Process batch (list of dicts)
        if i == 2: # Limit example output
            break

finally:
    connector.close()
