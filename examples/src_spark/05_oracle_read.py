"""Multi-Source Oracle Query Execution via Spark Temporary Views with JDBC Partitioning & Fetching.

This script demonstrates how to execute optimized cross-database queries across multiple Oracle sources
using `SparkConnector`. It configures parallel JDBC partitioning based on `ACCOUNT_ID`, enforces a batch
fetch size of 1,000 rows per round-trip, and inspects active Spark settings alongside physical execution plans.
"""

from src_connectors import SparkConnector
from src_connectors.exceptions import DependencyConflictError
from src_connectors.src_spark.configs.adapter.oracle import SparkOracleConfig
from src_connectors.src_spark.configs.core.core_config import SparkCoreConfig
import structlog

logger = structlog.get_logger(__name__)


def main() -> None:
    """Executes partitioned cross-database Oracle queries and logs execution metrics.

    Steps:
        1. Initialize baseline Spark engine configurations with custom SQL tuning parameters.
        2. Configure session-level Oracle driver details.
        3. Define targeted Oracle database configurations with JDBC partitioning and fetch sizes.
        4. Register database subqueries as Spark Temporary Views.
        5. Log active Spark settings and analyze physical execution plans (extended explain).
        6. Execute the final ANSI SQL query and display top results.
    """
    core_cfg = SparkCoreConfig(
        spark_app_name="Multi_Oracle_Direct_Call",
        spark_master="local[2]",
        spark_jars_packages="org.apache.spark:spark-avro_2.12:3.5.0",
        spark_sql_shuffle_partitions=10,
        spark_sql_execution_arrow_pyspark_enabled=True,
        spark_sql_autoBroadcastJoinThreshold=10 * 1024 * 1024,
    )

    oracle_session_cfg = SparkOracleConfig(oracle_driver_version="19.3.0.0")

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

    connector = SparkConnector(config=core_cfg)
    connector.configure_oracle(oracle_session_cfg)

    try:
        print("🔄 Initializing SparkSession...")
        spark = connector.connect()
        print("✅ SparkSession successfully established!\n")

        print("=" * 70)
        print("🛠️ ACTIVE SPARK CONFIGURATIONS CHECK")
        print("=" * 70)

        target_configs = [
            "spark.app.name",
            "spark.master",
            "spark.jars.packages",
            "spark.sql.shuffle.partitions",
            "spark.sql.execution.arrow.pyspark.enabled",
            "spark.sql.autoBroadcastJoinThreshold",
            "spark.driver.memory",
            "spark.executor.memory",
        ]

        for cfg_key in target_configs:
            val = spark.conf.get(cfg_key, "NOT_SET")
            print(f"🔹 {cfg_key:<45}: {val}")
        print("=" * 70 + "\n")

        print("🔗 Registering database sources as Spark Temp Views...")
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

        query = """
            SELECT
                a.ACCOUNT_ID,
                a.ACCOUNT_NUMBER, 
                s.VEHICLE_ID
            FROM v_account a
            LEFT JOIN v_subscribe s
                ON a.ACCOUNT_ID = s.ACCOUNT_ID
            WHERE s.ACCOUNT_ID IS NOT NULL
        """

        df = spark.sql(query)

        print("=" * 70)
        print("📊 SPARK EXECUTION PLAN (EXTENDED EXPLAIN)")
        print("=" * 70)
        df.explain(extended=True)
        print("=" * 70 + "\n")

        print("⚡ Executing Spark SQL Result:")
        df.show(10, truncate=False)

    except DependencyConflictError as e:
        print(f"❌ Dependency conflict detected: {e}")
    except Exception as e:
        print(f"⚠️ Runtime error occurred: {e}")
    finally:
        connector.close()


if __name__ == "__main__":
    main()
