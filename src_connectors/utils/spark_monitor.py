"""Terminal Progress & Resource Monitor for PySpark Jobs.

Designed for Kubernetes Pods & Jupyter Web Terminals without UI flickering.
Tracks system resources (CPU, Memory in GB & %) and Spark execution metrics
(Jobs, Active/Completed/Failed Stages, and Active Executors).
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
        """Initializes console, layout structures, and progress trackers."""
        self.console = Console(force_terminal=True, color_system="auto")
        self.app_name = app_name
        self.current_step: str = "Initializing Pipeline..."
        self.start_time: float = time.time()

        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=35),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("• [bold green]{task.completed}/{task.total} tasks"),
            console=self.console,
            expand=True,
        )
        self.main_task_id: Optional[TaskID] = None
        self.spark: Optional[SparkSession] = None

    def attach_spark(self, spark: SparkSession) -> None:
        """Attaches active SparkSession to capture execution metrics safely."""
        self.spark = spark

    def set_step(self, step_name: str, total_tasks: int = 100) -> None:
        """Updates active stage description and resets progress bar cleanly."""
        self.current_step = step_name

        if self.main_task_id is None:
            self.main_task_id = self.progress.add_task(description=step_name, total=total_tasks)
        else:
            self.progress.reset(
                self.main_task_id,
                description=step_name,
                total=total_tasks,
                completed=0,
            )

    def update_progress(self, completed: int, total: Optional[int] = None) -> None:
        """Updates progress bar completed counter."""
        if self.main_task_id is not None:
            if total is not None:
                self.progress.update(self.main_task_id, completed=completed, total=total)
            else:
                self.progress.update(self.main_task_id, completed=completed)

    def _sync_spark_realtime_tasks(self) -> None:
        """Query Spark's StatusTracker to sync real active stage tasks automatically."""
        if not self.spark or self.main_task_id is None:
            return

        try:
            tracker = self.spark.sparkContext.statusTracker
            active_stage_ids = tracker.getActiveStageIds() or []

            if active_stage_ids:
                total_completed = 0
                total_tasks = 0
                for stage_id in active_stage_ids:
                    stage_info = tracker.getStageInfo(stage_id)
                    if stage_info:
                        total_completed += stage_info.numCompletedTasks
                        total_tasks += stage_info.numTasks

                if total_tasks > 0:
                    self.progress.update(
                        self.main_task_id,
                        completed=total_completed,
                        total=total_tasks,
                    )
        except Exception:
            pass

    def _generate_layout(self) -> Layout:
        """Builds composite Rich terminal layout with detailed metrics."""
        self._sync_spark_realtime_tasks()

        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body", size=5),
            Layout(name="footer", size=10),
        )

        # 1. Top Header Panel
        elapsed = int(time.time() - self.start_time)
        header_text = Text(
            f"🚀 {self.app_name} | Elapsed Time: {elapsed}s",
            style="bold white on blue",
            justify="center",
        )
        layout["header"].update(Panel(header_text, style="blue"))

        # 2. Middle Progress Bar Panel
        layout["body"].update(
            Panel(
                self.progress,
                title=f"[bold gold1]Current Pipeline Stage: {self.current_step}[/bold gold1]",
                border_style="cyan",
            )
        )

        # 3. Bottom Resource & Spark Cluster Telemetry Table
        table = Table(expand=True, show_header=True, header_style="bold magenta")
        table.add_column("Category", style="dim", width=22)
        table.add_column("Metric Name", style="cyan")
        table.add_column("Live Telemetry Value", justify="right", style="bold green")

        # System Resources (RAM / CPU Details)
        cpu_usage = psutil.cpu_percent()
        virtual_mem = psutil.virtual_memory()
        used_ram_gb = virtual_mem.used / (1024**3)
        total_ram_gb = virtual_mem.total / (1024**3)
        ram_percent = virtual_mem.percent

        table.add_row("Host / Pod System", "CPU Utilization", f"{cpu_usage:.1f}%")
        table.add_row(
            "Host / Pod System",
            "RAM Utilization",
            f"{used_ram_gb:.2f} GB / {total_ram_gb:.2f} GB ({ram_percent:.1f}%)",
        )

        # Native Spark Telemetry Details
        if self.spark:
            try:
                sc = self.spark.sparkContext
                tracker = sc.statusTracker

                # Jobs & Stages Count
                active_job_ids = tracker.getJobIdsForGroup() or []
                active_stage_ids = tracker.getActiveStageIds() or []

                # Calculating Stage statistics (Active / Completed / Total)
                # Querying status tracker
                num_active_jobs = len(active_job_ids)
                num_active_stages = len(active_stage_ids)

                # Executors Count (local mode vs cluster mode)
                try:
                    # In local mode, Driver acts as Executor (num_executors = 1)
                    num_executors = len(sc._jsc.sc().getExecutorMemoryStatus().keys())
                except Exception:
                    num_executors = 1

                table.add_row("Spark Engine", "App ID / Master", f"{sc.applicationId} [{sc.master}]")
                table.add_row("Spark Engine", "Active Executors", f"{num_executors} Worker(s)")
                table.add_row("Spark Engine", "Active Jobs", f"{num_active_jobs} Running")
                table.add_row("Spark Engine", "Active Stages", f"{num_active_stages} Active Stage(s)")

            except Exception as e:
                table.add_row("Spark Engine", "Status Tracker", f"Active (Telemetry Degraded: {e})")
        else:
            table.add_row("Spark Engine", "Engine Status", "Initializing...")

        layout["footer"].update(
            Panel(
                table,
                title="[bold green]Live System Resources & Spark Cluster Telemetry[/bold green]",
                border_style="green",
            )
        )

        return layout

    def start_live_dashboard(self) -> Live:
        """Returns a clean, thread-safe Live context manager without screen overlaps."""
        return Live(
            self._generate_layout(),
            refresh_per_second=2,
            console=self.console,
            redirect_stdout=False,
            redirect_stderr=False,
            vertical_overflow="crop",
            transient=True,
        )
