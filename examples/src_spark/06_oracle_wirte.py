"""Oracle Database Write Execution via SparkConnector Encapsulated JDBC Writer.

This script demonstrates an end-to-end ETL pipeline:
1. Extracting data from multiple Oracle source databases in parallel using JDBC Partitioning.
2. Transforming data via Spark SQL JOINs and adding metadata audit columns.
3. Writing the transformed DataFrame into a target Oracle Data Warehouse (DWH) table
   using the encapsulated `write_jdbc_table` method with optimized batching.
"""

from src_connectors import SparkConnector
from src_connectors.exceptions import DependencyConflictError
from src_connectors.src_spark.configs.adapter.oracle import SparkOracleConfig
from src_connectors.src_spark.configs.core.core_config import SparkCoreConfig
import structlog

logger = structlog.get_logger(__name__)


def main() -> None:
    """Executes cross-database Oracle ETL and writes results to a target Oracle table.

    Steps:
        1. Initialize Spark engine baseline with performance configurations.
        2. Set up session-level Oracle JDBC driver details.
        3. Define connection properties for source and target Oracle databases.
        4. Register source database queries as Spark Temporary Views.
        5. Execute Spark SQL transformation to aggregate business metrics.
        6. Persist transformed results into Target Oracle DB via `connector.write_jdbc_table`.
    """
    # 1. Baseline Spark Core Configuration with Engine Tuning
    core_cfg = SparkCoreConfig(
        spark_app_name="Oracle_ETL_Write_Job",
        spark_master="local[2]",
        spark_jars_packages="org.apache.spark:spark-avro_2.12:3.5.0",
        spark_sql_shuffle_partitions=10,
        spark_sql_execution_arrow_pyspark_enabled=True,
        spark_driver_memory="4g",
    )

    # 2. Session-Level Driver Setup
    oracle_session_cfg = SparkOracleConfig(oracle_driver_version="19.3.0.0")

    # 3. Source Databases Configurations (Read Layer)
    src_1 = SparkOracleConfig(
        jdbc_url="jdbc:oracle:thin:@//localhost:1521/db1",
        jdbc_username="username",
        jdbc_password="password",
        fetch_size=1000,
        partition_column="ACCOUNT_ID",
        lower_bound=1,
        upper_bound=10000000,
        num_partitions=4,
    )

    src_2 = SparkOracleConfig(
        jdbc_url="jdbc:oracle:thin:@//localhost:1521/db2",
        jdbc_username="username",
        jdbc_password="password",
        fetch_size=1000,
        partition_column="ACCOUNT_ID",
        lower_bound=1,
        upper_bound=10000000,
        num_partitions=4,
    )

    # 4. Target Database Configuration (Write Layer)
    target_db = SparkOracleConfig(
        jdbc_url="jdbc:oracle:thin:@//localhost:1521/dwh_db",
        jdbc_username="dwh_user",
        jdbc_password="dwh_password",
    )

    # 5. Initialize SparkConnector
    connector = SparkConnector(config=core_cfg)
    connector.configure_oracle(oracle_session_cfg)

    try:
        print("🔄 Initializing SparkSession...")
        spark = connector.connect()
        print("✅ SparkSession successfully established!\n")

        print("🔗 Registering source database tables as Spark Temp Views...")
        connector.register_jdbc_view(
            view_name="v_account",
            config=src_1,
            dbtable_or_query="(SELECT * FROM crm_owner.account) t",
        )

        connector.register_jdbc_view(
            view_name="v_subscribe",
            config=src_2,
            dbtable_or_query="(SELECT * FROM crm_owner.subscriber) sub",
        )

        print("⚡ Executing Spark SQL Transformation...")
        transform_query = """
            SELECT
                a.ACCOUNT_ID,
                a.ACCOUNT_NUMBER, 
                s.VEHICLE_ID,
                CURRENT_TIMESTAMP() AS PROCESSED_AT
            FROM v_account a
            INNER JOIN v_subscribe s
                ON a.ACCOUNT_ID = s.ACCOUNT_ID
            WHERE s.STATUS = 'ACTIVE'
        """

        df_transformed = spark.sql(transform_query)

        print("\n" + "=" * 70)
        print("📊 TRANSFORMED DATAFRAME SAMPLE BEFORE WRITE")
        print("=" * 70)
        df_transformed.show(5, truncate=False)
        print("=" * 70 + "\n")

        # ---------------------------------------------------------------------
        # WRITE TO TARGET ORACLE DATABASE VIA CONNECTOR
        # ---------------------------------------------------------------------
        target_table_name = "DWH_OWNER.FACT_ACTIVE_SUBSCRIBERS"
        print(f"🚀 Writing transformed data to Target Oracle Table '{target_table_name}'...")

        connector.write_jdbc_table(
            df=df_transformed,
            config=target_db,
            target_table=target_table_name,
            mode="append",      # Options: 'append', 'overwrite'
            batch_size=5000     # Inserts 5,000 rows per transaction batch
        )

        print(f"✅ Data successfully written to {target_table_name}!")

    except DependencyConflictError as e:
        print(f"❌ Dependency conflict detected: {e}")
    except Exception as e:
        print(f"⚠️ Runtime error occurred during ETL process: {e}")
    finally:
        connector.close()


if __name__ == "__main__":
    main()
