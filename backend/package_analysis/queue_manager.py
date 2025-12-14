"""
Queue management system for dynamic analysis tasks.
Ensures only one container runs at a time by managing a queue of pending tasks.
"""

import threading
import time
from django.utils import timezone
from django.db import transaction
from .models import AnalysisTask
from .helper import Helper
from .container_manager import container_manager
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


class QueueManager:
    """
    Manages the analysis task queue to ensure only one container runs at a time.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(QueueManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._worker_thread = None
            self._stop_worker = threading.Event()
            self._worker_lock = threading.Lock()
            self._initialized = True
            logger.info("QueueManager initialized")
    
    def add_task_to_queue(self, task):
        """
        Add a task to the analysis queue.
        
        Args:
            task: AnalysisTask instance to add to queue
            
        Returns:
            int: Queue position of the task
        """
        with transaction.atomic():
            # Get the next queue position
            last_queued_task = AnalysisTask.objects.filter(
                status='queued'
            ).order_by('-queue_position').first()
            
            if last_queued_task and last_queued_task.queue_position:
                next_position = last_queued_task.queue_position + 1
            else:
                next_position = 1
            
            # Update task status and queue position
            task.status = 'queued'
            task.queue_position = next_position
            task.queued_at = timezone.now()
            task.save()
            
            logger.info(f"Task {task.id} added to queue at position {next_position}")
            
            # Start worker if not running
            self._start_worker_if_needed()
            
            return next_position
    
    def get_queue_status(self):
        """
        Get current queue status information.
        
        Returns:
            dict: Queue status information
        """
        with transaction.atomic():
            queued_tasks = AnalysisTask.objects.filter(
                status='queued'
            ).order_by('queue_position')
            
            running_tasks = AnalysisTask.objects.filter(
                status='running'
            )
            
            return {
                'queue_length': queued_tasks.count(),
                'running_tasks': running_tasks.count(),
                'queued_tasks': [
                    {
                        'task_id': task.id,
                        'purl': task.purl,
                        'queue_position': task.queue_position,
                        'priority': task.priority,
                        'queued_at': task.queued_at.isoformat() if task.queued_at else None,
                        'created_at': task.created_at.isoformat()
                    }
                    for task in queued_tasks
                ],
                'running_tasks': [
                    {
                        'task_id': task.id,
                        'purl': task.purl,
                        'started_at': task.started_at.isoformat() if task.started_at else None,
                        'created_at': task.created_at.isoformat()
                    }
                    for task in running_tasks
                ]
            }
    
    def get_task_queue_position(self, task_id):
        """
        Get the queue position of a specific task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            int or None: Queue position if task is queued, None otherwise
        """
        try:
            task = AnalysisTask.objects.get(id=task_id)
            if task.status == 'queued':
                return task.queue_position
            elif task.status == 'running':
                return 0  # Currently running
            else:
                return None  # Not in queue
        except AnalysisTask.DoesNotExist:
            return None
    
    def _start_worker_if_needed(self):
        """Start the worker thread if it's not already running."""
        with self._worker_lock:
            if self._worker_thread is None or not self._worker_thread.is_alive():
                self._stop_worker.clear()
                self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
                self._worker_thread.start()
                logger.info("Analysis worker thread started")
    
    def _worker_loop(self):
        """
        Main worker loop that processes queued tasks.
        Only one task runs at a time to ensure container resource management.
        """
        logger.info("Analysis worker loop started")
        
        while not self._stop_worker.is_set():
            try:
                # Check for timed out tasks first
                self.check_timeouts()
                
                # Get the next task in queue
                next_task = self._get_next_task()
                
                if next_task:
                    logger.info(f"Processing task {next_task.id} from queue")
                    self._process_task(next_task)
                else:
                    # No tasks in queue, wait a bit
                    time.sleep(5)
                    
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                time.sleep(10)  # Wait longer on error
        
        logger.info("Analysis worker loop stopped")
    
    def _get_next_task(self):
        """
        Get the next task to process from the queue.
        
        Returns:
            AnalysisTask or None: Next task to process
        """
        with transaction.atomic():
            # Check if there's already a running task
            running_task = AnalysisTask.objects.filter(status='running').first()
            if running_task:
                return None  # Wait for current task to complete
            
            # Get the next queued task (highest priority, then oldest)
            next_task = AnalysisTask.objects.filter(
                status='queued'
            ).order_by('-priority', 'queue_position').first()
            
            if next_task:
                # Double-check if a completed task already exists for this PURL
                # This prevents processing if a result was completed while in queue
                completed_task = AnalysisTask.objects.filter(
                    purl=next_task.purl,
                    status='completed',
                    report__isnull=False
                ).first()
                
                if completed_task:
                    # Mark the queued task as completed and link to existing report
                    logger.info(f"Task {next_task.id} already has completed result, marking as completed")
                    next_task.status = 'completed'
                    next_task.report = completed_task.report
                    next_task.download_url = completed_task.download_url
                    next_task.completed_at = timezone.now()
                    next_task.queue_position = None
                    next_task.save()
                    
                    # Update queue positions for remaining tasks
                    self._update_queue_positions()
                    
                    # Try to get the next task
                    return self._get_next_task()
            
            return next_task
    
    def _process_task(self, task):
        """
        Process a single analysis task.
        
        Args:
            task: AnalysisTask instance to process
        """
        try:
            # Update task status to running
            with transaction.atomic():
                task.status = 'running'
                task.started_at = timezone.now()
                task.last_heartbeat = timezone.now()
                task.save()
                
                # Remove from queue position
                task.queue_position = None
                task.save()
            
            logger.info(f"Started processing task {task.id}: {task.purl}")
            
            # Run the analysis
            from .models import ReportDynamicAnalysis
            from .views import save_report
            
            # Run the analysis using existing Helper methods
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor() as executor:
                future_reports = executor.submit(
                    Helper.run_package_analysis, 
                    task.package_name, 
                    task.package_version, 
                    task.ecosystem
                )
                reports = future_reports.result()
                
                # Try to extract container ID from the analysis process
                # This is a placeholder - you may need to modify Helper.run_package_analysis
                # to return container information
                try:
                    # Update heartbeat during processing
                    task.last_heartbeat = timezone.now()
                    task.save()
                except Exception as heartbeat_error:
                    logger.warning(f"Could not update heartbeat for task {task.id}: {heartbeat_error}")
            
            # Save the report
            save_report(reports)
            latest_report = ReportDynamicAnalysis.objects.latest('id')
            
            # Update task with completed status
            with transaction.atomic():
                task.status = 'completed'
                task.completed_at = timezone.now()
                task.report = latest_report
                task.save()
            
            # Save professional report
            try:
                from .views import save_professional_report
                
                # We need a request object for save_professional_report
                # For now, we'll create a minimal request-like object
                class MockRequest:
                    def build_absolute_uri(self, url):
                        return f"{getattr(settings, 'BASE_URL', 'http://localhost')}{url}"
                
                mock_request = MockRequest()
                download_url, report_metadata = save_professional_report(task, mock_request)
                task.download_url = download_url
                task.save()
                
                logger.info(f"Task {task.id} completed successfully. Download URL: {download_url}")
                
            except Exception as e:
                logger.warning(f"Failed to save professional report for task {task.id}: {e}")
            
            # Update queue positions for remaining tasks
            self._update_queue_positions()
            
        except Exception as e:
            # Handle analysis failure
            error_message = str(e)
            error_category = 'unknown_error'
            error_details = {}
            
            # Check if this is our custom AnalysisError with detailed information
            if hasattr(e, 'error_details'):
                error_details = e.error_details
                error_category = error_details.get('error_category', 'unknown_error')
                error_message = error_details.get('error_message', str(e))
            
            with transaction.atomic():
                task.status = 'failed'
                task.error_message = error_message
                task.error_category = error_category
                task.error_details = error_details
                task.completed_at = timezone.now()
                task.queue_position = None
                task.save()
            
            logger.error(f"Task {task.id} failed: {error_message}")
            
            # Update queue positions for remaining tasks
            self._update_queue_positions()
    
    def _update_queue_positions(self):
        """
        Update queue positions for all queued tasks after a task is processed.
        """
        with transaction.atomic():
            queued_tasks = AnalysisTask.objects.filter(
                status='queued'
            ).order_by('-priority', 'queued_at')
            
            for index, task in enumerate(queued_tasks, 1):
                task.queue_position = index
                task.save()
    
    def stop_worker(self):
        """Stop the worker thread."""
        with self._worker_lock:
            if self._worker_thread and self._worker_thread.is_alive():
                self._stop_worker.set()
                self._worker_thread.join(timeout=10)
                logger.info("Analysis worker thread stopped")
    
    def is_worker_running(self):
        """Check if the worker thread is running."""
        with self._worker_lock:
            return self._worker_thread is not None and self._worker_thread.is_alive()
    
    def check_timeouts(self):
        """
        Check for timed out tasks and handle them.
        This method should be called periodically to monitor running tasks.
        """
        try:
            with transaction.atomic():
                # Find all running tasks that have timed out
                running_tasks = AnalysisTask.objects.filter(status='running')
                timed_out_tasks = []
                
                for task in running_tasks:
                    if task.is_timed_out():
                        timed_out_tasks.append(task)
                
                # Handle each timed out task
                for task in timed_out_tasks:
                    logger.warning(f"Task {task.id} has timed out after {task.timeout_minutes} minutes")
                    self._handle_timed_out_task(task)
                
                if timed_out_tasks:
                    logger.info(f"Handled {len(timed_out_tasks)} timed out tasks")
                    
        except Exception as e:
            logger.error(f"Error checking timeouts: {e}")
    
    def _handle_timed_out_task(self, task):
        """
        Handle a task that has timed out.
        
        Args:
            task: AnalysisTask that has timed out
        """
        try:
            # Stop the container if it's still running
            if task.container_id:
                logger.info(f"Stopping timed out container {task.container_id} for task {task.id}")
                container_stopped = container_manager.stop_container(task.container_id)
                
                if container_stopped:
                    logger.info(f"Successfully stopped container {task.container_id}")
                else:
                    logger.warning(f"Failed to stop container {task.container_id}")
                
                # Try to get container logs for debugging
                try:
                    logs = container_manager.get_container_logs(task.container_id, tail=50)
                    logger.info(f"Container {task.container_id} logs (last 50 lines):\n{logs}")
                except Exception as log_error:
                    logger.warning(f"Could not retrieve logs for container {task.container_id}: {log_error}")
            
            # Update task status to failed
            task.status = 'failed'
            task.error_message = f"Task timed out after {task.timeout_minutes} minutes"
            task.error_category = 'timeout_error'
            task.error_details = {
                'timeout_minutes': task.timeout_minutes,
                'started_at': task.started_at.isoformat() if task.started_at else None,
                'timed_out_at': timezone.now().isoformat(),
                'container_id': task.container_id,
                'container_stopped': container_stopped if task.container_id else None
            }
            task.completed_at = timezone.now()
            task.queue_position = None
            task.save()
            
            logger.info(f"Marked task {task.id} as failed due to timeout")
            
            # Update queue positions for remaining tasks
            self._update_queue_positions()
            
        except Exception as e:
            logger.error(f"Error handling timed out task {task.id}: {e}")
    
    def get_timeout_status(self):
        """
        Get status of running tasks and their timeout information.
        
        Returns:
            Dictionary with timeout status information
        """
        try:
            running_tasks = AnalysisTask.objects.filter(status='running')
            timeout_info = []
            
            for task in running_tasks:
                remaining_time = task.get_remaining_time_minutes()
                is_timed_out = task.is_timed_out()
                
                timeout_info.append({
                    'task_id': task.id,
                    'purl': task.purl,
                    'started_at': task.started_at.isoformat() if task.started_at else None,
                    'timeout_minutes': task.timeout_minutes,
                    'remaining_minutes': remaining_time,
                    'is_timed_out': is_timed_out,
                    'container_id': task.container_id,
                    'container_running': container_manager.is_container_running(task.container_id) if task.container_id else False
                })
            
            return {
                'running_tasks': len(running_tasks),
                'timed_out_tasks': len([t for t in timeout_info if t['is_timed_out']]),
                'tasks': timeout_info
            }
            
        except Exception as e:
            logger.error(f"Error getting timeout status: {e}")
            return {'error': str(e)}


# Global queue manager instance
queue_manager = QueueManager()
