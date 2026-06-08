from src_connectors.src_sqlserver import SQLServerConnector

def run_example():
    sql_connector = SQLServerConnector()
    query = "SELECT TOP 20 * FROM db.schema.table"

    print("--- Case 6: Streaming, Output Type: dataframe, Batch Size: 10 ---")
    stream = sql_connector.execute_query(query, stream=True, batch_size=10, output_type="dataframe")
    for i, batch in enumerate(stream):
        print(f"Batch {i+1} Type: {type(batch)}")
        print(batch.head(1))

if __name__ == "__main__":
    run_example()
