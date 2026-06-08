from src_connectors.src_sqlserver import SQLServerConnector

def run_example():
    sql_connector = SQLServerConnector()
    query = "SELECT TOP 20 * FROM db.schema.table"

    print("--- Case 7: Unsupported Output Type ---")
    try:
        sql_connector.execute_query(query, output_type="xml")
    except ValueError as e:
        print(f"Caught expected error: {e}")

if __name__ == "__main__":
    run_example()
