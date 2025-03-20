"""Performance monitoring and metrics collection."""

import time
import threading
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import psutil
import matplotlib.pyplot as plt
from pathlib import Path
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PerformanceMetrics:
    """Container for performance metrics."""
    timestamp: datetime
    events_processed: int
    throughput: float
    api_calls: int
    db_operations: int
    memory_usage: float
    cpu_usage: float
    errors: int

class PerformanceMonitor:
    """Monitor and visualize processing performance."""

    def __init__(self, metrics_dir: Path):
        """Initialize performance monitor."""
        self.metrics_dir = metrics_dir
        self.metrics_dir.mkdir(exist_ok=True)
        
        self.metrics: List[PerformanceMetrics] = []
        self.start_time = datetime.now()
        self._stop_flag = threading.Event()
        self._monitor_thread = None

    def start_monitoring(self, interval: float = 1.0):
        """Start monitoring in background thread."""
        self._stop_flag.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            args=(interval,),
            daemon=True
        )
        self._monitor_thread.start()

    def stop_monitoring(self):
        """Stop monitoring and save results."""
        if self._monitor_thread:
            self._stop_flag.set()
            self._monitor_thread.join()
            self._save_metrics()

    def _monitor_loop(self, interval: float):
        """Continuous monitoring loop."""
        while not self._stop_flag.is_set():
            try:
                metrics = self._collect_metrics()
                self.metrics.append(metrics)
                self._update_dashboard(metrics)
                time.sleep(interval)
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")

    def _collect_metrics(self) -> PerformanceMetrics:
        """Collect current performance metrics."""
        process = psutil.Process()
        
        return PerformanceMetrics(
            timestamp=datetime.now(),
            events_processed=0,  # Updated from batch processor
            throughput=0.0,      # Updated from batch processor
            api_calls=0,         # Updated from batch processor
            db_operations=0,     # Updated from batch processor
            memory_usage=process.memory_info().rss / 1024 / 1024,  # MB
            cpu_usage=process.cpu_percent(),
            errors=0             # Updated from batch processor
        )

    def update_batch_metrics(self, batch_metrics: Dict):
        """Update metrics from batch processor."""
        if self.metrics:
            current = self.metrics[-1]
            current.events_processed = batch_metrics['events_processed']
            current.throughput = batch_metrics['throughput']
            current.api_calls = batch_metrics['api_calls']
            current.db_operations = batch_metrics['db_operations']
            current.errors = batch_metrics['errors']

    def _update_dashboard(self, metrics: PerformanceMetrics):
        """Update real-time performance dashboard."""
        self._plot_metrics()
        self._save_current_metrics(metrics)

    def _plot_metrics(self):
        """Generate performance plots."""
        if len(self.metrics) < 2:
            return

        # Create figure with subplots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        
        # Get timestamps for x-axis
        timestamps = [m.timestamp for m in self.metrics]
        
        # Plot throughput
        ax1.plot(timestamps, [m.throughput for m in self.metrics])
        ax1.set_title('Events Throughput')
        ax1.set_ylabel('Events/second')
        
        # Plot memory usage
        ax2.plot(timestamps, [m.memory_usage for m in self.metrics])
        ax2.set_title('Memory Usage')
        ax2.set_ylabel('MB')
        
        # Plot CPU usage
        ax3.plot(timestamps, [m.cpu_usage for m in self.metrics])
        ax3.set_title('CPU Usage')
        ax3.set_ylabel('Percent')
        
        # Plot error rate
        ax4.plot(timestamps, [m.errors for m in self.metrics])
        ax4.set_title('Cumulative Errors')
        
        # Adjust layout and save
        plt.tight_layout()
        plt.savefig(self.metrics_dir / 'performance_dashboard.png')
        plt.close()

    def _save_current_metrics(self, metrics: PerformanceMetrics):
        """Save current metrics to file."""
        current_metrics = {
            'timestamp': metrics.timestamp.isoformat(),
            'events_processed': metrics.events_processed,
            'throughput': metrics.throughput,
            'memory_usage_mb': metrics.memory_usage,
            'cpu_usage_percent': metrics.cpu_usage,
            'errors': metrics.errors
        }
        
        with open(self.metrics_dir / 'current_metrics.json', 'w') as f:
            json.dump(current_metrics, f, indent=2)

    def _save_metrics(self):
        """Save all collected metrics."""
        metrics_data = []
        for m in self.metrics:
            metrics_data.append({
                'timestamp': m.timestamp.isoformat(),
                'events_processed': m.events_processed,
                'throughput': m.throughput,
                'api_calls': m.api_calls,
                'db_operations': m.db_operations,
                'memory_usage_mb': m.memory_usage,
                'cpu_usage_percent': m.cpu_usage,
                'errors': m.errors
            })
            
        with open(self.metrics_dir / f'metrics_{self.start_time:%Y%m%d_%H%M%S}.json', 'w') as f:
            json.dump(metrics_data, f, indent=2)

    def get_summary(self) -> Dict:
        """Get summary of performance metrics."""
        if not self.metrics:
            return {}
            
        return {
            'duration': str(datetime.now() - self.start_time),
            'total_events': self.metrics[-1].events_processed,
            'avg_throughput': sum(m.throughput for m in self.metrics) / len(self.metrics),
            'max_throughput': max(m.throughput for m in self.metrics),
            'avg_memory_mb': sum(m.memory_usage for m in self.metrics) / len(self.metrics),
            'max_memory_mb': max(m.memory_usage for m in self.metrics),
            'total_errors': self.metrics[-1].errors,
            'total_api_calls': self.metrics[-1].api_calls,
            'total_db_ops': self.metrics[-1].db_operations
        }
