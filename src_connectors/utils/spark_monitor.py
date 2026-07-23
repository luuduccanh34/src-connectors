"""Terminal Progress & Resource Monitor for PySpark Jobs.

Designed for Kubernetes Pods & Jupyter Web Terminals without UI flickering.
Features clear step logging and a high-contrast, professional execution summary report.
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
    """Thread-safe Professional Terminal Dashboard & Telemetry Report for PySpark ETL."""

    def __init__(self, app_name: str = "Spark Data Pipeline") -> None:
        """Initializes console, layout structures, and telemetry history trackers.

        Args:
            app_name: Descriptive title for the pipeline dashboard.
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

        # Single-line progress bar (prevents multi-frame flickering in Web Terminals)
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold bright_cyan]{task.description}"),
            BarColumn(bar_width=35, complete_style="green", finished_style="bright_green"),
            TextColumn("[bold bright_white]{task.percentage:>3.0f}%"),
            console=self.console,
            transient=True,
        )
        self.main_task_id: Optional[TaskID] = None
        self.spark: Optional[SparkSession] = None

    def attach_spark(self, spark: SparkSession) -> None:
        """Attaches active SparkSession to capture cluster status telemetry.

        Args:
            spark: Active SparkSession instance.
        """
        self.spark = spark

    def start(self) -> None:
        """Starts the progress tracker timer and renders the live progress bar."""
        self.start_time = time.time()
        self.console.print(
            f"\n[bold white on blue] 🚀 STARTING PIPELINE: {self.app_name.upper()} [/bold white on blue]\n"
        )
        self.progress.start()
        self.main_task_id = self.progress.add_task(description=self.current_step, total=100)

    def set_step(self, step_name: str, completed: int = 0, total: int = 100) -> None:
        """Prints a clean log entry and updates current stage progress.

        Args:
            step_name: Description of current execution step.
            completed: Current completed task/unit count.
            total: Target total task/unit count.
        """
        self.current_step = step_name
        self._record_system_metrics()

        # In log cố định ra console để bảo lưu dấu vết bước chạy trên terminal
        self.console.print(
            f"[bold bright_black]►[/bold bright_black] [bold yellow]EXEC_STAGE:[/bold yellow] [bold white]{step_name}[/bold white]"
        )

        if self.main_task_id is not None:
            self.progress.update(
                self.main_task_id,
                description=step_name,
                completed=completed,
                total=total,
            )

    def update_progress(self, completed: int, total: Optional[int] = None) -> None:
        """Updates progress bar completed counter.

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

        if used_ram_gb > self.metrics_history["peak_ram_gb"]:
            self.metrics_history["peak_ram_gb"] = used_ram_gb
        if cpu_pct > self.metrics_history["peak_cpu_pct"]:
            self.metrics_history["peak_cpu_pct"] = cpu_pct

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
        """Stops progress tracking and displays a high-contrast, professional execution summary report.

        Args:
            status: Final status ('SUCCESS', 'FAILED', or 'WARNING').
            target_table: Destination table or path name.
            details: Optional explanatory notes or error messages.
        """
        self.end_time = time.time()
        self._record_system_metrics()
        self.progress.stop()

        elapsed = int(self.end_time - self.start_time)
        status_color = "bright_green" if status == "SUCCESS" else "bright_red"
        status_badge = (
            f"[bold white on green] {status} [/bold white on green]"
            if status == "SUCCESS"
            else f"[bold white on red] {status} [/bold white on red]"
        )

        # 1. Table: Data Pipeline & I/O Telemetry
        t1 = Table(show_header=True, header_style="bold bright_cyan", expand=True, box=None)
        t1.add_column("Pipeline Metric", style="bright_white", width=28)
        t1.add_column("Telemetry Value", justify="right")

        t1.add_row("Pipeline Application Name", f"[bold white]{self.app_name}[/bold white]")
        t1.add_row("Final Job Status", status_badge)
        t1.add_row(
            "Total Execution Time",
            f"[bold bright_yellow]{elapsed}s[/bold bright_yellow] ({elapsed // 60}m {elapsed % 60}s)",
        )
        t1.add_row("Target Storage / Table", f"[bold green]{target_table}[/bold green]")
        t1.add_section()
        t1.add_row(
            "Source Total Rows Read",
            f"[bold bright_cyan]{self.metrics_history['rows_read']:,}[/bold bright_cyan] records",
        )
        t1.add_row(
            "Target Total Rows Written",
            f"[bold bright_green]{self.metrics_history['rows_written']:,}[/bold bright_green] records",
        )

        # 2. Table: System Resources & Spark Cluster Telemetry
        t2 = Table(show_header=True, header_style="bold bright_cyan", expand=True, box=None)
        t2.add_column("Resource / Engine Metric", style="bright_white", width=28)
        t2.add_column("Telemetry Value", justify="right")

        virtual_mem = psutil.virtual_memory()
        total_ram_gb = virtual_mem.total / (1024**3)
        ram_pct = (self.metrics_history["peak_ram_gb"] / total_ram_gb) * 100

        t2.add_row(
            "Peak Host RAM Usage",
            f"[bold bright_magenta]{self.metrics_history['peak_ram_gb']:.2f} GB[/bold bright_magenta] / {total_ram_gb:.2f} GB ({ram_pct:.1f}%)",
        )
        t2.add_row(
            "Peak Host CPU Usage",
            f"[bold bright_magenta]{self.metrics_history['peak_cpu_pct']:.1f}%[/bold bright_magenta]",
        )
        t2.add_section()

        if self.spark:
            try:
                sc = self.spark.sparkContext
                try:
                    num_executors = len(sc._jsc.sc().getExecutorMemoryStatus().keys())
                except Exception:
                    num_executors = 1

                t2.add_row("Spark Application ID", f"[dim]{sc.applicationId}[/dim]")
                t2.add_row("Spark Master Mode", f"[bold white]{sc.master}[/bold white]")
                t2.add_row("Active Executors", f"[bold white]{num_executors} Worker(s)[/bold white]")
            except Exception:
                pass

        if details:
            t2.add_section()
            t2.add_row("Execution Notes", f"[italic bright_white]{details}[/italic bright_white]")

        # Master Table Layout Wrapping
        master_table = Table(show_header=False, expand=True, box=None)
        master_table.add_column("Col")
        master_table.add_row(
            Panel(
                t1,
                title="[bold bright_white]1. DATA PIPELINE & I/O TELEMETRY[/bold bright_white]",
                border_style="bright_blue",
            )
        )
        master_table.add_row(
            Panel(
                t2,
                title="[bold bright_white]2. HARDWARE RESOURCES & SPARK ENGINE[/bold bright_white]",
                border_style="bright_blue",
            )
        )

        summary_panel = Panel(
            master_table,
            title=" [bold white on blue] 📊 PIPELINE EXECUTION SUMMARY REPORT [/bold white on blue] ",
            border_style=status_color,
            subtitle=f"[{status_color}]Completed at {time.strftime('%Y-%m-%d %H:%M:%S')}[/{status_color}]",
        )

        self.console.print("\n")
        self.console.print(summary_panel)
        self.console.print("\n")
