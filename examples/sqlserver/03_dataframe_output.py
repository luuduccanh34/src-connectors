from src_connectors.src_sqlserver import SQLServerConnector

def run_example():
    sql_connector = SQLServerConnector()
    query = "SELECT TOP 20 * FROM db.schema.table"

    print("--- Case 3: Non-streaming, Output Type: dataframe ---")
    df = sql_connector.execute_query(query, output_type="dataframe")
    print(f"Type: {type(df)}")
    print(df.head(2))

if __name__ == "__main__":
    run_example()
