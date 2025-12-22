"""Utility module for resource monitoring and profiling."""

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import List, Optional

import psutil

logger = logging.getLogger(__name__)

@dataclass
class ResourceMetrics:
    """Snapshot of resource usage."""
    cpu_percent: float
    memory_rss_mb: float
    memory_percent: float
    timestamp: float

class ResourceMonitor:
    """Background monitor for CPU and Memory usage."""

    def __init__(self, interval: float = 1.0):
        self.interval = interval
        self.metrics: List[ResourceMetrics] = []
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._process = psutil.Process(os.getpid())

    def _monitor(self):
        """Monitor loop running in background thread."""
        while not self._stop_event.is_set():
            try:
                # Get metrics for the current process and its children
                cpu = self._process.cpu_percent(interval=None)
                mem_info = self._process.memory_info()
                mem_percent = self._process.memory_percent()
                
                # Include children usage if any (important for multiprocessing)
                for child in self._process.children(recursive=True):
                    try:
                        cpu += child.cpu_percent(interval=None)
                        mem_info_child = child.memory_info()
                        mem_info = mem_info._replace(rss=mem_info.rss + mem_info_child.rss)
                        mem_percent += child.memory_percent()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue

                self.metrics.append(ResourceMetrics(
                    cpu_percent=cpu,
                    memory_rss_mb=mem_info.rss / (1024 * 1024),
                    memory_percent=mem_percent,
                    timestamp=time.time()
                ))
            except Exception as e:
                logger.error(f"Error in monitor: {e}")
            
            time.sleep(self.interval)

    def start(self):
        """Start the background monitor."""
        self.metrics = []
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._monitor, daemon=True)
        self._thread.start()
        logger.debug("Resource monitor started.")

    def stop(self) -> List[ResourceMetrics]:
        """Stop the monitor and return collected metrics."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        logger.debug("Resource monitor stopped.")
        return self.metrics

    def get_summary(self) -> dict:
        """Calculate summary statistics from collected metrics."""
        if not self.metrics:
            return {}
        
        cpus = [m.cpu_percent for m in self.metrics]
        mems = [m.memory_rss_mb for m in self.metrics]
        
        return {
            "avg_cpu": sum(cpus) / len(cpus),
            "max_cpu": max(cpus),
            "avg_mem_mb": sum(mems) / len(mems),
            "max_mem_mb": max(mems),
            "duration_sec": self.metrics[-1].timestamp - self.metrics[0].timestamp,
            "samples": len(self.metrics)
        }

def log_resource_summary(name: str, summary: dict):
    """Log the resource summary in a readable format."""
    if not summary:
        return
    
    logger.info(
        f"Resource Summary [{name}]: "
        f"CPU Avg: {summary['avg_cpu']:.1f}%, Max: {summary['max_cpu']:.1f}% | "
        f"MEM Avg: {summary['avg_mem_mb']:.1f}MB, Max: {summary['max_mem_mb']:.1f}MB | "
        f"Duration: {summary['duration_sec']:.1f}s"
    )
