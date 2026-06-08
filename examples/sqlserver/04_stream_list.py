from src_connectors.src_sqlserver import SQLServerConnector

def run_example():
    sql_connector = SQLServerConnector()
    query = "SELECT TOP 20 * FROM db.schema.table"

    print("--- Case 4: Streaming, Output Type: list, Batch Size: 5 ---")
    stream = sql_connector.execute_query(query, stream=True, batch_size=5, output_type="list")
    for i, batch in enumerate(stream):
        print(f"Batch {i+1} Type: {type(batch)}, Size: {len(batch)}")

if __name__ == "__main__":
    run_example()
