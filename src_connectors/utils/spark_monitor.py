"""Terminal Progress & Resource Monitor for PySpark Jobs.

This module provides thread-safe real-time progress tracking, Spark metrics monitoring,
and system resource utilization using standard PySpark Status APIs and Rich.
"""

import time
from typing import Optional
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


class PipelineMonitor:
    """Thread-safe Terminal Dashboard for PySpark ETL Execution Monitoring."""

    def __init__(self, app_name: str = "Spark Data Pipeline") -> None:
        """Initializes console, layout structures, and progress trackers.

        Args:
            app_name: Descriptive title for the pipeline dashboard.
        """
        # Standard Console initialization
        self.console = Console()
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
        self.spark: Optional[SparkSession] = None

    def attach_spark(self, spark: SparkSession) -> None:
        """Attaches active SparkSession to capture execution metrics safely via Status Tracker.

        Args:
            spark: Active SparkSession instance.
        """
        self.spark = spark

    def set_step(self, step_name: str, total_tasks: int = 100) -> None:
        """Updates active stage description and resets progress bar.

        Args:
            step_name: Title/description of current step.
            total_tasks: Target completed task count.
        """
        self.current_step = step_name
        if self.main_task_id is None:
            self.main_task_id = self.progress.add_task(description=step_name, total=total_tasks)
        else:
            self.progress.reset(self.main_task_id, description=step_name, total=total_tasks, completed=0)

    def update_progress(self, completed: int) -> None:
        """Updates progress bar completed counter.

        Args:
            completed: Incremental or absolute completed task count.
        """
        if self.main_task_id is not None:
            self.progress.update(self.main_task_id, completed=completed)

    def _generate_layout(self) -> Layout:
        """Builds composite Rich terminal layout."""
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

        # 3. Bottom Resource and Metrics Panel
        table = Table(expand=True, show_header=True, header_style="bold magenta")
        table.add_column("Resource / Metric Name", style="dim")
        table.add_column("Live Telemetry Value", justify="right")

        # Capture Host System Resources
        cpu_usage = psutil.cpu_percent()
        ram_usage = psutil.virtual_memory().percent

        table.add_row("Driver Host CPU Utilization", f"{cpu_usage:.1f}%")
        table.add_row("Driver Host RAM Utilization", f"{ram_usage:.1f}%")

        # Native PySpark Status Metrics
        if self.spark:
            try:
                status_tracker = self.spark.sparkContext.statusTracker
                active_jobs = len(status_tracker.getJobIdsForGroup() or [])
                active_stages = len(status_tracker.getActiveStageIds() or [])
                table.add_row("Active Spark Jobs", f"{active_jobs} running")
                table.add_row("Active Execution Stages", f"{active_stages} stages")
            except Exception:
                table.add_row("Spark Engine Status", "Active")
        else:
            table.add_row("Spark Engine Status", "Initializing")

        layout["footer"].update(
            Panel(
                table,
                title="[bold green]Live Engine Telemetry & System Resources[/bold green]",
                border_style="green",
            )
        )

        return layout

    def start_live_dashboard(self) -> Live:
        """Returns configured thread-safe Live context manager for UI rendering."""
        return Live(
            self._generate_layout(),
            refresh_per_second=2,
            console=self.console,
            redirect_stdout=True,
            redirect_stderr=True,
            vertical_overflow="crop",
        )
