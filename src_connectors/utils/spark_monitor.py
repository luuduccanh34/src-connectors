"""Terminal Progress & Resource Monitor for PySpark Jobs using Rich and SparkListener.

This module provides real-time progress tracking, Spark metrics monitoring (Records Read/Written,
Data Volumes), and system resource usage (Driver CPU/RAM) using `rich` live layout components.
"""

import time
from typing import Optional, Any
import psutil
from pyspark.sql import SparkSession
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID, TextColumn
from rich.table import Table
from rich.text import Text
import structlog

logger = structlog.get_logger(__name__)


class SparkTaskListener:
    """Captures real-time metrics (Tasks, I/O records, Bytes) from the Spark Engine via Py4J Gateway."""

    def __init__(self, spark: SparkSession) -> None:
        """Initializes the Py4J Java listener bridge.

        Args:
            spark: Active SparkSession instance.
        """
        self.spark = spark
        self.completed_tasks: int = 0
        self.records_read: int = 0
        self.records_written: int = 0
        self.bytes_read: int = 0
        self.bytes_written: int = 0

        try:
            sc = self.spark.sparkContext
            gateway = sc._gateway

            # Define dynamic Java proxy implementing org.apache.spark.scheduler.SparkListener
            class Listener(object):
                def __init__(listener_self) -> None:
                    pass

                def onTaskEnd(listener_self, taskEnd: Any) -> None:
                    metrics = taskEnd.taskMetrics()
                    if metrics:
                        # Capture Input Metrics (Read Operations)
                        input_metrics = metrics.inputMetrics()
                        if input_metrics:
                            self.records_read += input_metrics.recordsRead()
                            self.bytes_read += input_metrics.bytesRead()

                        # Capture Output Metrics (Write Operations)
                        output_metrics = metrics.outputMetrics()
                        if output_metrics:
                            self.records_written += output_metrics.recordsWritten()
                            self.bytes_written += output_metrics.bytesWritten()

                    self.completed_tasks += 1

                class Java:
                    implements = ["org.apache.spark.scheduler.SparkListener"]

            self._listener = Listener()
            # Register custom listener directly to Spark Context
            sc._jsc.sc().addSparkListener(
                gateway.jvm.org.apache.spark.scheduler.SparkListener(self._listener)
            )
            logger.debug("Successfully registered SparkTaskListener to SparkContext")
        except Exception as e:
            logger.warning("Failed to register SparkTaskListener. Metrics will default to 0", error=str(e))


class PipelineMonitor:
    """Manages thread-safe Rich Live UI Terminal Dashboard for Spark Execution Monitoring."""

    def __init__(self, app_name: str = "Spark Data Pipeline") -> None:
        """Initializes console, layout structures, and progress trackers.

        Args:
            app_name: Descriptive title for the pipeline dashboard.
        """
        # Critical: Redirect stdout and stderr to prevent console flickering and duplicate frames
        self.console = Console(redirect_stdout=True, redirect_stderr=True)
        self.app_name = app_name
        self.current_step: str = "Initializing Pipeline..."
        self.start_time: float = time.time()

        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("• [bold green]{task.completed}/{task.total} tasks"),
            console=self.console,
        )
        self.main_task_id: Optional[TaskID] = None
        self.listener: Optional[SparkTaskListener] = None

    def attach_spark(self, spark: SparkSession) -> None:
        """Attaches active SparkSession to capture execution metrics.

        Args:
            spark: Active SparkSession instance.
        """
        self.listener = SparkTaskListener(spark)

    def set_step(self, step_name: str, total_tasks: int = 100) -> None:
        """Updates the active stage description and resets the task progress bar.

        Args:
            step_name: The title/description of the current step.
            total_tasks: Target completed task count for progress calculation.
        """
        self.current_step = step_name
        if self.main_task_id is None:
            self.main_task_id = self.progress.add_task(description=step_name, total=total_tasks)
        else:
            self.progress.reset(self.main_task_id, description=step_name, total=total_tasks, completed=0)

    def update_progress(self, completed: int) -> None:
        """Updates progress bar completed counter.

        Args:
            completed: Incremental or absolute completed task unit count.
        """
        if self.main_task_id is not None:
            self.progress.update(self.main_task_id, completed=completed)

    def _generate_layout(self) -> Layout:
        """Builds the composite Rich terminal layout with Header, Body, and Metrics panels."""
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body", size=6),
            Layout(name="footer", size=7),
        )

        # 1. Top Header Panel
        elapsed = int(time.time() - self.start_time)
        header_text = Text(
            f"🚀 {self.app_name} | Elapsed: {elapsed}s",
            style="bold white on blue",
            justify="center",
        )
        layout["header"].update(Panel(header_text))

        # 2. Middle Progress Bar Panel
        layout["body"].update(
            Panel(
                self.progress,
                title=f"[bold gold1]Current Stage: {self.current_step}[/bold gold1]",
                border_style="cyan",
            )
        )

        # 3. Bottom Resource and I/O Metrics Table Panel
        table = Table(expand=True, show_header=True, header_style="bold magenta")
        table.add_column("Resource / Metric Name", style="dim")
        table.add_column("Live Telemetry Value", justify="right")

        # Capture Host / Driver System Resources
        cpu_usage = psutil.cpu_percent()
        ram_usage = psutil.virtual_memory().percent

        table.add_row("Driver Host CPU Utilization", f"{cpu_usage:.1f}%")
        table.add_row("Driver Host RAM Utilization", f"{ram_usage:.1f}%")

        if self.listener:
            mb_read = self.listener.bytes_read / (1024 * 1024)
            mb_written = self.listener.bytes_written / (1024 * 1024)
            table.add_row(
                "Rows Read via JDBC (Oracle)",
                f"{self.listener.records_read:,} records ({mb_read:.2f} MB)",
            )
            table.add_row(
                "Rows Written via JDBC (Target DB)",
                f"{self.listener.records_written:,} records ({mb_written:.2f} MB)",
            )
        else:
            table.add_row("Rows Read via JDBC (Oracle)", "0 records (0.00 MB)")
            table.add_row("Rows Written via JDBC (Target DB)", "0 records (0.00 MB)")

        layout["footer"].update(
            Panel(
                table,
                title="[bold green]Live Engine Telemetry & System Resources[/bold green]",
                border_style="green",
            )
        )

        return layout

    def start_live_dashboard(self) -> Live:
        """Returns a configured thread-safe Live context manager for continuous rendering.

        Returns:
            Rich Live context manager instance.
        """
        return Live(
            self._generate_layout(),
            refresh_per_second=4,
            console=self.console,
            redirect_stdout=True,
            redirect_stderr=True,
            vertical_overflow="crop",
        )
