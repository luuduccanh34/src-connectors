"""Terminal Progress & Resource Monitor for PySpark Jobs using Rich and SparkListener."""

import time
import psutil
from typing import Optional
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TaskID
from rich.table import Table
from rich.text import Text
from pyspark.sql import SparkSession


class SparkTaskListener:
    """Java SparkListener bridge to capture real-time task metrics from Spark Engine."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.total_tasks = 0
        self.completed_tasks = 0
        self.records_read = 0
        self.records_written = 0
        self.bytes_read = 0
        self.bytes_written = 0

        # Gateway bridge to Py4J Listener
        try:
            sc = self.spark.sparkContext
            gateway = sc._gateway

            # Create dynamic Java proxy for SparkListener
            class Listener(object):
                def __init__(listener_self):
                    pass

                def onTaskEnd(listener_self, taskEnd):
                    metrics = taskEnd.taskMetrics()
                    if metrics:
                        # Capture Input Metrics
                        input_metrics = metrics.inputMetrics()
                        if input_metrics:
                            self.records_read += input_metrics.recordsRead()
                            self.bytes_read += input_metrics.bytesRead()

                        # Capture Output Metrics
                        output_metrics = metrics.outputMetrics()
                        if output_metrics:
                            self.records_written += output_metrics.recordsWritten()
                            self.bytes_written += output_metrics.bytesWritten()

                    self.completed_tasks += 1

                class Java:
                    implements = ["org.apache.spark.scheduler.SparkListener"]

            self._listener = Listener()
            # Register listener to Spark Context
            sc._jsc.sc().addSparkListener(gateway.jvm.org.apache.spark.scheduler.SparkListener(self._listener))
        except Exception:
            # Fallback for local testing if Py4J proxy fails
            pass


class PipelineMonitor:
    """Live Terminal Dashboard Manager."""

    def __init__(self, app_name: str = "Spark Data Pipeline"):
        self.console = Console()
        self.app_name = app_name
        self.current_step = "Initializing Pipeline..."
        self.start_time = time.time()

        # Rich UI Components
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("• [bold green]{task.completed}/{task.total} tasks"),
        )
        self.main_task_id: Optional[TaskID] = None
        self.listener: Optional[SparkTaskListener] = None

    def attach_spark(self, spark: SparkSession) -> None:
        """Attaches Spark context to track active metrics."""
        self.listener = SparkTaskListener(spark)

    def set_step(self, step_name: str, total_tasks: int = 100) -> None:
        """Updates the active pipeline stage name and task progress bar."""
        self.current_step = step_name
        if self.main_task_id is None:
            self.main_task_id = self.progress.add_task(description=step_name, total=total_tasks)
        else:
            self.progress.reset(self.main_task_id, description=step_name, total=total_tasks, completed=0)

    def update_progress(self, completed: int) -> None:
        """Updates completed tasks count."""
        if self.main_task_id is not None:
            self.progress.update(self.main_task_id, completed=completed)

    def _generate_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body", size=10),
            Layout(name="footer", size=6),
        )

        # Header Panel
        elapsed = int(time.time() - self.start_time)
        header_text = Text(f"🚀 {self.app_name} | Elapsed: {elapsed}s", style="bold white on blue", justify="center")
        layout["header"].update(Panel(header_text))

        # Body: Step Status & Progress Bar
        layout["body"].update(
            Panel(
                self.progress,
                title=f"[bold gold1]Current Step: {self.current_step}[/bold gold1]",
                border_style="cyan",
            )
        )

        # Footer Table: Metrics & System Resources
        table = Table(expand=True, show_header=True, header_style="bold magenta")
        table.add_column("Resource / Metric", style="dim")
        table.add_column("Current Value", justify="right")

        # Get Driver System Resource
        cpu_usage = psutil.cpu_percent()
        ram_usage = psutil.virtual_memory().percent

        table.add_row("Driver CPU Usage", f"{cpu_usage}%")
        table.add_row("Driver RAM Usage", f"{ram_usage}%")

        if self.listener:
            mb_read = self.listener.bytes_read / (1024 * 1024)
            mb_written = self.listener.bytes_written / (1024 * 1024)
            table.add_row("Rows Read (Oracle JDBC)", f"{self.listener.records_read:,} ({mb_read:.2f} MB)")
            table.add_row("Rows Written (Target DB)", f"{self.listener.records_written:,} ({mb_written:.2f} MB)")
        else:
            table.add_row("Rows Read (Oracle JDBC)", "0")
            table.add_row("Rows Written (Target DB)", "0")

        layout["footer"].update(
            Panel(table, title="[bold green]Live Metrics & System Resources[/bold green]", border_style="green"))

        return layout

    def start_live_dashboard(self):
        """Returns a Live context manager to display the dashboard continuously."""
        return Live(self._generate_layout(), refresh_per_second=2, console=self.console)
