"""
Dynamic thread pool manager for MASX AI ETL CPU Pipeline.

Provides intelligent thread pool management with CPU scaling, monitoring,
and graceful shutdown capabilities for high-performance parallel processing.
"""

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Any, List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging

from ..config.settings import settings


logger = logging.getLogger(__name__)


@dataclass
class WorkerStats:
    """Statistics for worker thread performance."""
    worker_id: str
    tasks_completed: int = 0
    total_processing_time: float = 0.0
    last_activity: datetime = None
    current_task: Optional[str] = None
    
    @property
    def average_processing_time(self) -> float:
        """Calculate average processing time per task."""
        if self.tasks_completed == 0:
            return 0.0
        return self.total_processing_time / self.tasks_completed
    
    @property
    def is_idle(self) -> bool:
        """Check if worker is currently idle."""
        if self.last_activity is None:
            return True
        return time.time() - self.last_activity.timestamp() > 60  # 1 minute idle threshold


class DynamicThreadPool:
    """
    Dynamic thread pool with CPU scaling and performance monitoring.
    
    Automatically adjusts thread count based on CPU cores and workload,
    provides detailed statistics, and handles graceful shutdown.
    """
    
    def __init__(self, max_workers: Optional[int] = None):
        """
        Initialize the dynamic thread pool.
        
        Args:
            max_workers: Maximum number of workers (defaults to CPU-based calculation)
        """
        self.max_workers = max_workers or settings.max_workers
        self.min_workers = 1
        self.current_workers = self.min_workers
        
        # Thread pool and monitoring
        self._executor: Optional[ThreadPoolExecutor] = None
        self._worker_stats: Dict[str, WorkerStats] = {}
        self._stats_lock = threading.Lock()
        self._shutdown_event = threading.Event()
        
        # Performance tracking
        self._total_tasks_submitted = 0
        self._total_tasks_completed = 0
        self._total_processing_time = 0.0
        self._start_time = None
        
        # Auto-scaling configuration
        self._scale_up_threshold = 0.8  # Scale up when 80% of workers are busy
        self._scale_down_threshold = 0.3  # Scale down when 30% of workers are busy
        self._last_scale_time = time.time()
        self._scale_cooldown = 30  # Minimum seconds between scaling operations
        
        logger.info(f"Initialized DynamicThreadPool with max_workers={self.max_workers}")
    
    def _create_executor(self) -> ThreadPoolExecutor:
        """Create a new ThreadPoolExecutor with current worker count."""
        return ThreadPoolExecutor(
            max_workers=self.current_workers,
            thread_name_prefix="masx-worker"
        )
    
    def _get_worker_id(self, thread_name: str) -> str:
        """Extract worker ID from thread name."""
        return thread_name.split("-")[-1] if "-" in thread_name else "unknown"
    
    def _update_worker_stats(
        self, 
        worker_id: str, 
        task_name: str, 
        processing_time: float
    ) -> None:
        """Update worker statistics in a thread-safe manner."""
        with self._stats_lock:
            if worker_id not in self._worker_stats:
                self._worker_stats[worker_id] = WorkerStats(worker_id=worker_id)
            
            stats = self._worker_stats[worker_id]
            stats.tasks_completed += 1
            stats.total_processing_time += processing_time
            stats.last_activity = datetime.now()
            stats.current_task = task_name
            
            self._total_tasks_completed += 1
            self._total_processing_time += processing_time
    
    def _should_scale_up(self) -> bool:
        """Determine if we should scale up the number of workers."""
        if self.current_workers >= self.max_workers:
            return False
        
        if time.time() - self._last_scale_time < self._scale_cooldown:
            return False
        
        with self._stats_lock:
            if not self._worker_stats:
                return False
            
            # Check if most workers are busy
            busy_workers = sum(1 for stats in self._worker_stats.values() if not stats.is_idle)
            busy_ratio = busy_workers / len(self._worker_stats)
            
            return busy_ratio >= self._scale_up_threshold
    
    def _should_scale_down(self) -> bool:
        """Determine if we should scale down the number of workers."""
        if self.current_workers <= self.min_workers:
            return False
        
        if time.time() - self._last_scale_time < self._scale_cooldown:
            return False
        
        with self._stats_lock:
            if not self._worker_stats:
                return False
            
            # Check if most workers are idle
            idle_workers = sum(1 for stats in self._worker_stats.values() if stats.is_idle)
            idle_ratio = idle_workers / len(self._worker_stats)
            
            return idle_ratio >= self._scale_down_threshold
    
    def _auto_scale(self) -> None:
        """Automatically adjust the number of workers based on workload."""
        if self._should_scale_up():
            old_count = self.current_workers
            self.current_workers = min(self.current_workers + 1, self.max_workers)
            if self.current_workers != old_count:
                logger.info(f"Auto-scaling UP: {old_count} -> {self.current_workers} workers")
                self._last_scale_time = time.time()
                self._restart_executor()
        
        elif self._should_scale_down():
            old_count = self.current_workers
            self.current_workers = max(self.current_workers - 1, self.min_workers)
            if self.current_workers != old_count:
                logger.info(f"Auto-scaling DOWN: {old_count} -> {self.current_workers} workers")
                self._last_scale_time = time.time()
                self._restart_executor()
    
    def _restart_executor(self) -> None:
        """Restart the executor with new worker count."""
        if self._executor:
            self._executor.shutdown(wait=False)
        self._executor = self._create_executor()
    
    def start(self) -> None:
        """Start the thread pool."""
        if self._executor is None:
            self._executor = self._create_executor()
            self._start_time = datetime.now()
            logger.info(f"Started thread pool with {self.current_workers} workers")
    
    def shutdown(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        Shutdown the thread pool gracefully.
        
        Args:
            wait: Whether to wait for running tasks to complete
            timeout: Maximum time to wait for shutdown
        """
        self._shutdown_event.set()
        
        if self._executor:
            self._executor.shutdown(wait=wait, timeout=timeout)
            self._executor = None
        
        logger.info("Thread pool shutdown completed")
    
    def submit_task(
        self, 
        func: Callable, 
        *args, 
        task_name: str = "unknown",
        **kwargs
    ) -> Any:
        """
        Submit a task to the thread pool.
        
        Args:
            func: Function to execute
            *args: Positional arguments for the function
            task_name: Human-readable name for the task
            **kwargs: Keyword arguments for the function
            
        Returns:
            Future object representing the task
        """
        if self._executor is None:
            raise RuntimeError("Thread pool not started")
        
        if self._shutdown_event.is_set():
            raise RuntimeError("Thread pool is shutting down")
        
        # Auto-scale if needed
        self._auto_scale()
        
        # Wrap the function to track statistics
        def tracked_func(*fargs, **fkwargs):
            start_time = time.time()
            worker_id = self._get_worker_id(threading.current_thread().name)
            
            try:
                result = func(*fargs, **fkwargs)
                processing_time = time.time() - start_time
                self._update_worker_stats(worker_id, task_name, processing_time)
                return result
            except Exception as e:
                processing_time = time.time() - start_time
                self._update_worker_stats(worker_id, f"{task_name} (ERROR)", processing_time)
                raise
        
        self._total_tasks_submitted += 1
        return self._executor.submit(tracked_func, *args, **kwargs)
    
    def map_tasks(
        self, 
        func: Callable, 
        iterable: List[Any], 
        task_name: str = "batch"
    ) -> List[Any]:
        """
        Execute a function on multiple items in parallel.
        
        Args:
            func: Function to execute
            iterable: List of items to process
            task_name: Human-readable name for the batch
            
        Returns:
            List of results in the same order as input
        """
        if not iterable:
            return []
        
        futures = []
        for i, item in enumerate(iterable):
            future = self.submit_task(
                func, 
                item, 
                task_name=f"{task_name}_{i+1}"
            )
            futures.append(future)
        
        results = []
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Task failed: {e}")
                results.append(None)
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the thread pool.
        
        Returns:
            Dictionary with detailed statistics
        """
        with self._stats_lock:
            current_time = datetime.now()
            uptime = (current_time - self._start_time).total_seconds() if self._start_time else 0
            
            # Calculate worker statistics
            worker_stats = {}
            for worker_id, stats in self._worker_stats.items():
                worker_stats[worker_id] = {
                    "tasks_completed": stats.tasks_completed,
                    "average_processing_time": stats.average_processing_time,
                    "is_idle": stats.is_idle,
                    "current_task": stats.current_task,
                    "last_activity": stats.last_activity.isoformat() if stats.last_activity else None
                }
            
            # Calculate overall statistics
            total_workers = len(self._worker_stats)
            busy_workers = sum(1 for stats in self._worker_stats.values() if not stats.is_idle)
            
            return {
                "pool_status": {
                    "current_workers": self.current_workers,
                    "max_workers": self.max_workers,
                    "min_workers": self.min_workers,
                    "total_workers": total_workers,
                    "busy_workers": busy_workers,
                    "idle_workers": total_workers - busy_workers,
                    "is_running": self._executor is not None and not self._shutdown_event.is_set()
                },
                "performance": {
                    "total_tasks_submitted": self._total_tasks_submitted,
                    "total_tasks_completed": self._total_tasks_completed,
                    "total_processing_time": self._total_processing_time,
                    "average_processing_time": (
                        self._total_processing_time / self._total_tasks_completed 
                        if self._total_tasks_completed > 0 else 0
                    ),
                    "tasks_per_second": (
                        self._total_tasks_completed / uptime 
                        if uptime > 0 else 0
                    ),
                    "uptime_seconds": uptime
                },
                "worker_details": worker_stats
            }
    
    def is_healthy(self) -> bool:
        """
        Check if the thread pool is healthy and responsive.
        
        Returns:
            True if healthy, False otherwise
        """
        if self._executor is None or self._shutdown_event.is_set():
            return False
        
        # Check if we have any active workers
        with self._stats_lock:
            if not self._worker_stats:
                return True  # No workers yet, but pool is running
            
            # Check if any workers are responsive
            current_time = time.time()
            for stats in self._worker_stats.values():
                if stats.last_activity and (current_time - stats.last_activity.timestamp()) < 300:  # 5 minutes
                    return True
            
            # If we have workers but none are responsive, something might be wrong
            return len(self._worker_stats) == 0


# Global thread pool instance
thread_pool = DynamicThreadPool()
