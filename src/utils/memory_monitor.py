"""
Memory monitoring utilities for MASX AI ETL CPU Pipeline.

Provides comprehensive memory monitoring, leak detection, and cleanup utilities
to ensure optimal memory usage during batch processing.
"""

import gc
import psutil
import os
import asyncio
from typing import Dict, Any, Optional, Callable
from datetime import datetime
from contextlib import asynccontextmanager
from dataclasses import dataclass

from src.config import get_service_logger

logger = get_service_logger(__name__)


@dataclass
class MemorySnapshot:
    """Memory usage snapshot at a point in time."""
    
    timestamp: datetime
    rss_mb: float  # Resident Set Size in MB
    vms_mb: float  # Virtual Memory Size in MB
    percent: float  # Memory usage percentage
    available_mb: float  # Available system memory in MB
    
    @property
    def memory_pressure(self) -> str:
        """Determine memory pressure level."""
        if self.percent > 90:
            return "critical"
        elif self.percent > 80:
            return "high"
        elif self.percent > 70:
            return "moderate"
        else:
            return "low"


class MemoryMonitor:
    """
    Comprehensive memory monitoring and cleanup utility.
    
    Provides memory tracking, leak detection, and automatic cleanup
    for high-performance batch processing scenarios.
    """
    
    def __init__(self, process_id: Optional[int] = None):
        """
        Initialize memory monitor.
        
        Args:
            process_id: Process ID to monitor (defaults to current process)
        """
        self.process_id = process_id or os.getpid()
        self.process = psutil.Process(self.process_id)
        self.snapshots: list[MemorySnapshot] = []
        self.cleanup_callbacks: list[Callable] = []
        
        logger.info(f"Memory monitor initialized for PID {self.process_id}")
    
    def take_snapshot(self, label: str = "") -> MemorySnapshot:
        """
        Take a memory usage snapshot.
        
        Args:
            label: Optional label for the snapshot
            
        Returns:
            MemorySnapshot object with current memory state
        """
        try:
            memory_info = self.process.memory_info()
            system_memory = psutil.virtual_memory()
            
            snapshot = MemorySnapshot(
                timestamp=datetime.now(),
                rss_mb=memory_info.rss / 1024 / 1024,
                vms_mb=memory_info.vms / 1024 / 1024,
                percent=self.process.memory_percent(),
                available_mb=system_memory.available / 1024 / 1024
            )
            
            self.snapshots.append(snapshot)
            
            logger.info(
                f"Memory snapshot {label}: {snapshot.rss_mb:.2f} MB RSS, "
                f"{snapshot.percent:.1f}% usage, pressure: {snapshot.memory_pressure}"
            )
            
            return snapshot
            
        except Exception as e:
            logger.error(f"Error taking memory snapshot: {e}")
            return None
    
    def get_memory_delta(self, start_snapshot: MemorySnapshot, 
                        end_snapshot: MemorySnapshot) -> Dict[str, Any]:
        """
        Calculate memory delta between two snapshots.
        
        Args:
            start_snapshot: Initial memory snapshot
            end_snapshot: Final memory snapshot
            
        Returns:
            Dictionary with memory delta information
        """
        if not start_snapshot or not end_snapshot:
            return {"error": "Invalid snapshots"}
        
        rss_delta = end_snapshot.rss_mb - start_snapshot.rss_mb
        percent_delta = end_snapshot.percent - start_snapshot.percent
        
        return {
            "rss_delta_mb": rss_delta,
            "percent_delta": percent_delta,
            "duration_seconds": (end_snapshot.timestamp - start_snapshot.timestamp).total_seconds(),
            "memory_leak_detected": rss_delta > 100,  # More than 100MB increase
            "high_memory_usage": end_snapshot.percent > 80
        }
    
    def force_cleanup(self) -> Dict[str, Any]:
        """
        Force aggressive memory cleanup.
        
        Returns:
            Dictionary with cleanup results
        """
        initial_memory = self.take_snapshot("before_cleanup")
        
        try:
            # Force garbage collection multiple times
            for i in range(3):
                collected = gc.collect()
                logger.debug(f"GC cycle {i+1}: collected {collected} objects")
            
            # Call registered cleanup callbacks
            for callback in self.cleanup_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        asyncio.create_task(callback())
                    else:
                        callback()
                except Exception as e:
                    logger.error(f"Error in cleanup callback: {e}")
            
            final_memory = self.take_snapshot("after_cleanup")
            
            if initial_memory and final_memory:
                delta = self.get_memory_delta(initial_memory, final_memory)
                logger.info(f"Memory cleanup completed: {delta['rss_delta_mb']:+.2f} MB")
                return delta
            
            return {"status": "cleanup_completed"}
            
        except Exception as e:
            logger.error(f"Error during memory cleanup: {e}")
            return {"error": str(e)}
    
    def register_cleanup_callback(self, callback: Callable):
        """
        Register a cleanup callback function.
        
        Args:
            callback: Function to call during cleanup
        """
        self.cleanup_callbacks.append(callback)
        logger.debug(f"Registered cleanup callback: {callback.__name__}")
    
    def check_memory_pressure(self) -> str:
        """
        Check current memory pressure level.
        
        Returns:
            Memory pressure level string
        """
        snapshot = self.take_snapshot("pressure_check")
        return snapshot.memory_pressure if snapshot else "unknown"
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive memory statistics.
        
        Returns:
            Dictionary with detailed memory information
        """
        try:
            memory_info = self.process.memory_info()
            system_memory = psutil.virtual_memory()
            
            return {
                "process_memory": {
                    "rss_mb": memory_info.rss / 1024 / 1024,
                    "vms_mb": memory_info.vms / 1024 / 1024,
                    "percent": self.process.memory_percent(),
                    "num_threads": self.process.num_threads(),
                    "num_fds": self.process.num_fds() if hasattr(self.process, 'num_fds') else 0
                },
                "system_memory": {
                    "total_mb": system_memory.total / 1024 / 1024,
                    "available_mb": system_memory.available / 1024 / 1024,
                    "percent": system_memory.percent,
                    "used_mb": system_memory.used / 1024 / 1024
                },
                "gc_stats": {
                    "counts": gc.get_count(),
                    "thresholds": gc.get_threshold()
                },
                "snapshots_count": len(self.snapshots)
            }
            
        except Exception as e:
            logger.error(f"Error getting memory stats: {e}")
            return {"error": str(e)}
    
    def clear_snapshots(self):
        """Clear all stored memory snapshots."""
        self.snapshots.clear()
        logger.debug("Memory snapshots cleared")


@asynccontextmanager
async def memory_monitored_operation(operation_name: str = "operation"):
    """
    Context manager for memory-monitored operations.
    
    Args:
        operation_name: Name of the operation for logging
        
    Yields:
        MemoryMonitor instance
    """
    monitor = MemoryMonitor()
    start_snapshot = monitor.take_snapshot(f"{operation_name}_start")
    
    try:
        yield monitor
    finally:
        end_snapshot = monitor.take_snapshot(f"{operation_name}_end")
        
        if start_snapshot and end_snapshot:
            delta = monitor.get_memory_delta(start_snapshot, end_snapshot)
            
            if delta.get("memory_leak_detected"):
                logger.warning(f"Potential memory leak detected in {operation_name}")
                monitor.force_cleanup()
            
            logger.info(
                f"Operation {operation_name} completed: "
                f"{delta.get('rss_delta_mb', 0):+.2f} MB memory delta"
            )


# Global memory monitor instance
memory_monitor = MemoryMonitor()


def get_memory_monitor() -> MemoryMonitor:
    """Get the global memory monitor instance."""
    return memory_monitor
