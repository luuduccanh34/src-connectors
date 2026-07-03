import sys
import os

# Ensure the root of the project is in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src_connectors import TrinoConnector

def run_example():
    # Khởi tạo TrinoConnector, cấu hình sẽ được tự động load từ Environment Variables
    # (TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_PASSWORD) thông qua TrinoVariables
    trino_connector = TrinoConnector()

    query = "SELECT * FROM precomp.ns_ewallet.customer_open_account_details LIMIT 10"

    print("--- Trino Query Example ---")
    print(f"Query: {query}")

    print("\n--- Output Type: dataframe ---")
    df_results = trino_connector.execute_query(query, output_type="dataframe")
    print(f"Type: {type(df_results)}")
    print(df_results)

    print("\n--- Output Type: list ---")
    list_results = trino_connector.execute_query(query, output_type="list")
    print(f"Type: {type(list_results)}, Count: {len(list_results)}")
    if list_results:
        print(f"First row: {list_results[0]}")

if __name__ == "__main__":
    run_example()
