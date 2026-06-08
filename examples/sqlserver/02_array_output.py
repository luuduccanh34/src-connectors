from src_connectors.src_sqlserver import SQLServerConnector

def run_example():
    sql_connector = SQLServerConnector()
    query = "SELECT TOP 20 * FROM db.schema.table"

    print("--- Case 2: Non-streaming, Output Type: array ---")
    results = sql_connector.execute_query(query, output_type="array")
    print(f"Type: {type(results)}, Count: {len(results)}")
    if results:
        print(f"First row: {results[0]}")

if __name__ == "__main__":
    run_example()
