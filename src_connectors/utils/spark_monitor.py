"""Terminal Progress & Resource Monitor for PySpark Jobs.

Designed for Kubernetes Pods & Jupyter Web Terminals without UI flickering.
Features a clean single-line progress indicator during execution and displays
a persistent, detailed summary report upon pipeline completion.
"""

import time
from typing import Any, Dict, Optional
import psutil
from pyspark.sql import SparkSession
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID, TextColumn
from rich.table import Table
import structlog

logger = structlog.get_logger(__name__)


class PipelineMonitor:
    """Thread-safe Terminal Progress Tracker and Summary Reporter for PySpark ETL."""

    def __init__(self, app_name: str = "Spark Data Pipeline") -> None:
        """Initializes console and telemetry history trackers.

        Args:
            app_name: Title of the pipeline.
        """
        self.console = Console(force_terminal=True, color_system="auto")
        self.app_name = app_name
        self.current_step: str = "Initializing Pipeline..."
        self.start_time: float = time.time()
        self.end_time: Optional[float] = None

        # Telemetry history for the final persistent summary report
        self.metrics_history: Dict[str, Any] = {
            "rows_read": 0,
            "rows_written": 0,
            "peak_ram_gb": 0.0,
            "peak_cpu_pct": 0.0,
            "total_jobs": 0,
            "total_stages": 0,
        }

        # Single-line progress indicator (Prevents multi-frame flickering in Web Terminals)
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=30),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            console=self.console,
            transient=True,  # Automatically cleans up execution bar to make room for Summary Table
        )
        self.main_task_id: Optional[TaskID] = None
        self.spark: Optional[SparkSession] = None

    def attach_spark(self, spark: SparkSession) -> None:
        """Attaches active SparkSession to capture telemetry.

        Args:
            spark: Active SparkSession instance.
        """
        self.spark = spark

    def start(self) -> None:
        """Starts the progress tracker timer and renders the live progress bar."""
        self.start_time = time.time()
        self.progress.start()
        self.main_task_id = self.progress.add_task(description=self.current_step, total=100)

    def set_step(self, step_name: str, completed: int = 0, total: int = 100) -> None:
        """Updates active step description and progress status.

        Args:
            step_name: Description of current execution step.
            completed: Current completed task/unit count.
            total: Target total task/unit count.
        """
        self.current_step = step_name
        self._record_system_metrics()

        if self.main_task_id is not None:
            self.progress.update(
                self.main_task_id,
                description=step_name,
                completed=completed,
                total=total,
            )

    def update_progress(self, completed: int, total: Optional[int] = None) -> None:
        """Updates completed progress counter.

        Args:
            completed: Incremental or absolute completed unit count.
            total: Optional total task count override.
        """
        self._record_system_metrics()
        if self.main_task_id is not None:
            if total is not None:
                self.progress.update(self.main_task_id, completed=completed, total=total)
            else:
                self.progress.update(self.main_task_id, completed=completed)

    def _record_system_metrics(self) -> None:
        """Captures peak RAM/CPU usage and Spark execution statistics in background."""
        virtual_mem = psutil.virtual_memory()
        used_ram_gb = virtual_mem.used / (1024**3)
        cpu_pct = psutil.cpu_percent()

        # Update peak resource metrics
        if used_ram_gb > self.metrics_history["peak_ram_gb"]:
            self.metrics_history["peak_ram_gb"] = used_ram_gb
        if cpu_pct > self.metrics_history["peak_cpu_pct"]:
            self.metrics_history["peak_cpu_pct"] = cpu_pct

        # Capture Spark Jobs and Stages if active
        if self.spark:
            try:
                tracker = self.spark.sparkContext.statusTracker
                self.metrics_history["total_jobs"] = len(tracker.getJobIdsForGroup() or [])
                self.metrics_history["total_stages"] = len(tracker.getActiveStageIds() or [])
            except Exception:
                pass

    def record_io_metrics(self, rows_read: int = 0, rows_written: int = 0) -> None:
        """Records data row volumes for the final summary telemetry.

        Args:
            rows_read: Number of rows read from source databases.
            rows_written: Number of rows written to target database/storage.
        """
        if rows_read > 0:
            self.metrics_history["rows_read"] += rows_read
        if rows_written > 0:
            self.metrics_history["rows_written"] += rows_written

    def stop_and_show_summary(
        self,
        status: str = "SUCCESS",
        target_table: str = "N/A",
        details: Optional[str] = None,
    ) -> None:
        """Stops progress tracking and displays a persistent Summary Report panel.

        Args:
            status: Final status ('SUCCESS', 'FAILED', or 'WARNING').
            target_table: Destination table or path name.
            details: Optional explanatory notes or error messages.
        """
        self.end_time = time.time()
        self._record_system_metrics()
        self.progress.stop()

        elapsed = int(self.end_time - self.start_time)
        status_style = "bold green" if status == "SUCCESS" else "bold red"

        # Construct Persistent Summary Table
        table = Table(expand=True, show_header=True, header_style="bold magenta")
        table.add_column("Category", style="dim", width=20)
        table.add_column("Telemetry Metric", style="cyan")
        table.add_column("Final Summary Value", justify="right", style="bold yellow")

        # 1. Pipeline Execution Status
        table.add_row("Execution Info", "Pipeline Name", self.app_name)
        table.add_row("Execution Info", "Final Status", f"[{status_style}]{status}[/{status_style}]")
        table.add_row("Execution Info", "Total Duration", f"{elapsed} seconds")
        table.add_row("Execution Info", "Target Table", target_table)

        # 2. Data Volume Telemetry
        table.add_row("Data Telemetry", "Source Rows Read", f"{self.metrics_history['rows_read']:,} rows")
        table.add_row("Data Telemetry", "Target Rows Written", f"{self.metrics_history['rows_written']:,} rows")

        # 3. System & Resource Metrics
        virtual_mem = psutil.virtual_memory()
        total_ram_gb = virtual_mem.total / (1024**3)
        ram_pct = (self.metrics_history["peak_ram_gb"] / total_ram_gb) * 100

        table.add_row(
            "Resource Usage",
            "Peak RAM Utilization",
            f"{self.metrics_history['peak_ram_gb']:.2f} GB / {total_ram_gb:.2f} GB ({ram_pct:.1f}%)",
        )
        table.add_row("Resource Usage", "Peak CPU Utilization", f"{self.metrics_history['peak_cpu_pct']:.1f}%")

        # 4. Spark Engine Details
        if self.spark:
            try:
                sc = self.spark.sparkContext

                # Active Executors Count
                try:
                    num_executors = len(sc._jsc.sc().getExecutorMemoryStatus().keys())
                except Exception:
                    num_executors = 1

                table.add_row("Spark Engine", "Application ID", sc.applicationId)
                table.add_row("Spark Engine", "Master Mode", sc.master)
                table.add_row("Spark Engine", "Executors Count", f"{num_executors} Worker(s)")
            except Exception:
                pass

        if details:
            table.add_row("Additional Notes", "Details", details)

        panel = Panel(
            table,
            title=f"[{status_style}]📊 PIPELINE EXECUTION SUMMARY REPORT[/{status_style}]",
            border_style="green" if status == "SUCCESS" else "red",
        )
        self.console.print("\n")
        self.console.print(panel)
        self.console.print("\n")
