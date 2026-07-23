"""Oracle ETL Job with Dynamic K8s Pod IP Resolution & Persistent Summary Report."""

import os
import socket
import time
import structlog

from src_connectors import SparkConnector
from src_connectors.exceptions import DependencyConflictError
from src_connectors.src_spark.configs.adapter.oracle import SparkOracleConfig
from src_connectors.src_spark.configs.core.core_config import SparkCoreConfig
from src_connectors.utils.spark_monitor import PipelineMonitor

logger = structlog.get_logger(__name__)


def get_k8s_pod_ip() -> str:
    """Dynamically resolves the internal Kubernetes Pod IP address.

    Falls back to resolving current hostname or local loopback if
    running outside a K8s container.

    Returns:
        str: Resolvable IP address for Spark BlockManager communication.
    """
    try:
        # K8s injects POD_IP env if configured, otherwise fallback to hostname resolution
        return os.getenv("POD_IP", socket.gethostbyname(socket.gethostname()))
    except Exception:
        return "127.0.0.1"


def main() -> None:
    target_table_name = "STAGING.CANHLD_TEST_WRITE_FRAMEWORK"

    # -------------------------------------------------------------------------
    # 1. PRE-INITIALIZE SPARK SESSION ENGINE
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("🔄 PRE-INITIALIZING SPARK SESSION ENGINE (K8S POD COMPATIBLE)...")
    print("=" * 80)

    pod_ip = get_k8s_pod_ip()
    logger.info("Resolved Driver Network Endpoint", resolved_pod_ip=pod_ip)

    core_cfg = SparkCoreConfig(
        spark_app_name="Oracle_ETL_Live_Monitored",
        spark_master="local[4]",
        spark_jars_packages="org.apache.spark:spark-avro_2.12:3.5.0",
        spark_sql_shuffle_partitions=10,
        spark_sql_execution_arrow_pyspark_enabled=True,
        spark_driver_memory="4g",
        spark_driver_host=pod_ip,
        spark_driver_bind_address="0.0.0.0",
        extra_spark_configs={
            # Essential JVM options for Java 17 reflection & Netty in K8s
            "spark.driver.extraJavaOptions": "-Djava.net.preferIPv4Stack=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "spark.executor.extraJavaOptions": "-Djava.net.preferIPv4Stack=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "spark.network.timeout": "800s",
            "spark.executor.heartbeatInterval": "60s",
        },
    )

    oracle_session_cfg = SparkOracleConfig(oracle_driver_version="19.3.0.0")

    connector = SparkConnector(config=core_cfg)
    connector.configure_oracle(oracle_session_cfg)

    spark = connector.connect()
    print("✅ Spark Session Engine Successfully Established!\n")

    # -------------------------------------------------------------------------
    # 2. INITIALIZE PIPELINE MONITOR & EXECUTE ETL
    # -------------------------------------------------------------------------
    monitor = PipelineMonitor(app_name="Oracle DWH ETL Pipeline")
    monitor.attach_spark(spark)
    monitor.start()

    try:
        # STEP 1
        monitor.set_step("Step 1/4: Spark Session Verified", completed=100, total=100)
        logger.info(
            "Spark Engine Details",
            app_id=spark.sparkContext.applicationId,
            version=spark.version,
            master=spark.sparkContext.master,
            driver_host=spark.conf.get("spark.driver.host", "UNKNOWN"),
        )
        time.sleep(1)

        # STEP 2
        monitor.set_step("Step 2/4: Registering Oracle JDBC Views", completed=1, total=4)

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

        connector.register_jdbc_view(
            view_name="v_account",
            config=src_1,
            dbtable_or_query="(SELECT * FROM crm_owner.account) t",
        )
        account_count = spark.sql("SELECT COUNT(1) FROM v_account").collect()[0][0]
        logger.info("Source View Registered", view="v_account", record_count=account_count)
        monitor.update_progress(completed=2)

        connector.register_jdbc_view(
            view_name="v_subscribe",
            config=src_2,
            dbtable_or_query="(SELECT * FROM crm_owner.subscriber) sub",
        )
        sub_count = spark.sql("SELECT COUNT(1) FROM v_subscribe").collect()[0][0]

        monitor.record_io_metrics(rows_read=(account_count + sub_count))
        monitor.update_progress(completed=4)
        time.sleep(1)

        # STEP 3
        monitor.set_step("Step 3/4: Executing Spark SQL Transformations & Joins", completed=10, total=100)

        transform_query = """
            SELECT
                a.ACCOUNT_ID,
                a.ACCOUNT_NUMBER, 
                s.VEHICLE_ID,
                CURRENT_TIMESTAMP() AS PROCESSED_AT
            FROM v_account a
            INNER JOIN v_subscribe s
                ON a.ACCOUNT_ID = s.ACCOUNT_ID
            WHERE UPPER(TRIM(s.STATUS)) = 'ACTIVE' OR s.STATUS = '1'
        """

        df_transformed = spark.sql(transform_query)
        output_count = df_transformed.count()
        logger.info("Transformation Complete", output_records=output_count)
        monitor.update_progress(completed=100)
        time.sleep(1)

        # STEP 4
        monitor.set_step("Step 4/4: Persisting Results to Target Oracle Table", completed=10, total=100)

        if output_count > 0:
            target_db = SparkOracleConfig(
                jdbc_url="jdbc:oracle:thin:@//localhost:1521/db",
                jdbc_username="username",
                jdbc_password="passwork",
            )

            connector.write_jdbc_table(
                df=df_transformed,
                config=target_db,
                target_table=target_table_name,
                mode="append",
                batch_size=5000,
            )

            monitor.record_io_metrics(rows_written=output_count)
            monitor.update_progress(completed=100)

            monitor.stop_and_show_summary(
                status="SUCCESS",
                target_table=target_table_name,
                details=f"Successfully written {output_count:,} rows into target table.",
            )
        else:
            monitor.stop_and_show_summary(
                status="WARNING",
                target_table=target_table_name,
                details="Transformation yielded 0 records. JDBC write operation skipped.",
            )

    except DependencyConflictError as e:
        logger.error("Dependency Error", error=str(e))
        monitor.stop_and_show_summary(
            status="FAILED",
            target_table=target_table_name,
            details=f"Dependency Exception: {e}",
        )
    except Exception as e:
        logger.error("Pipeline Runtime Exception", error=str(e))
        monitor.stop_and_show_summary(
            status="FAILED",
            target_table=target_table_name,
            details=f"Pipeline Error: {e}",
        )
    finally:
        connector.close()


if __name__ == "__main__":
    main()
