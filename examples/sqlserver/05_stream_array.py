from src_connectors.src_sqlserver import SQLServerConnector

def run_example():
    sql_connector = SQLServerConnector()
    query = "SELECT TOP 20 * FROM db.schema.table"

    print("--- Case 5: Streaming, Output Type: array, Batch Size: 7 ---")
    stream = sql_connector.execute_query(query, stream=True, batch_size=7, output_type="array")
    for i, batch in enumerate(stream):
        print(f"Batch {i+1} Type: {type(batch)}, Size: {len(batch)}")

if __name__ == "__main__":
    run_example()
