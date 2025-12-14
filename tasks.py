"""
Celery tasks for package analysis.
Migrated from QueueManager to use Celery for better scalability and monitoring.
"""

from celery import shared_task
from celery.exceptions import Retry
from .helper import Helper
from .models import AnalysisTask
from .container_manager import container_manager
from django.utils import timezone
from django.db import transaction
from django.core.cache import cache
from django.conf import settings
import logging
import traceback
from datetime import timedelta

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=1, default_retry_delay=60)
def run_dynamic_analysis(self, task_id):
    """
    Background task for dynamic analysis with single-container execution guarantee.
    
    This task ensures only one container runs at a time by checking for running tasks
    before processing. If another task is running, this task will be retried.
    
    Args:
        self: Celery task instance (bind=True)
        task_id: ID of AnalysisTask model instance
    
    Returns:
        dict: Status and results
    """
    logger.info(f"🚀 Worker {self.request.hostname} starting task {task_id}")
    
    try:
        with transaction.atomic():
            # Get task from database with lock
            task = AnalysisTask.objects.select_for_update().get(id=task_id)
            
            # CRITICAL: Check if another task is already running
            # This ensures only one container runs at a time
            running_task = AnalysisTask.objects.filter(
                status='running'
            ).exclude(id=task_id).first()
            
            if running_task:
                logger.info(f"⏸️  Another task {running_task.id} is running. Retrying task {task_id} in 30s...")
                # Retry after 30 seconds to allow current task to complete
                raise self.retry(countdown=30, exc=Exception("Another task is running"))
            
            # Check if task was already completed (race condition protection)
            if task.status == 'completed':
                logger.info(f"✅ Task {task_id} already completed, skipping")
                return {
                    'status': 'success',
                    'task_id': task_id,
                    'cached': True,
                    'message': 'Task already completed'
                }
            
            # Check for existing completed result for this PURL (smart caching)
            if task.purl:
                completed_task = AnalysisTask.objects.filter(
                    purl=task.purl,
                    status='completed',
                    report__isnull=False
                ).exclude(id=task_id).order_by('-completed_at').first()
                
                if completed_task:
                    logger.info(f"✅ Found existing completed result for {task.purl}, reusing")
                    task.status = 'completed'
                    task.completed_at = timezone.now()
                    task.report = completed_task.report
                    task.download_url = completed_task.download_url
                    task.queue_position = None
                    task.save()
                    
                    # Process next queued task
                    _process_next_queued_task()
                    
                    return {
                        'status': 'success',
                        'task_id': task_id,
                        'cached': True,
                        'message': 'Reused existing result'
                    }
            
            # Update task status to running
            task.status = 'running'
            task.started_at = timezone.now()
            task.queue_position = None  # Remove from queue position
            task.last_heartbeat = timezone.now()
            task.save()
        
        logger.info(f"📦 Analyzing {task.package_name}@{task.package_version} ({task.ecosystem})")
        
        # Check cache first
        cache_key = f"analysis_{task.ecosystem}_{task.package_name}_{task.package_version}"
        cached_result = cache.get(cache_key)
        
        if cached_result:
            logger.info(f"✅ Using cached result for {task.package_name}@{task.package_version}")
            with transaction.atomic():
                task.status = 'completed'
                task.completed_at = timezone.now()
                task.duration_seconds = 0.1  # Cache hit
                task.result = cached_result
                task.save()
            
            # Process next queued task
            _process_next_queued_task()
            
            return {
                'status': 'success',
                'task_id': task_id,
                'cached': True
            }
        
        # Run analysis
        start_time = timezone.now()
        
        try:
            results = Helper.run_package_analysis(
                package_name=task.package_name,
                package_version=task.package_version,
                ecosystem=task.ecosystem
            )
            
            # Try to extract container ID if available
            # This is a placeholder - modify Helper.run_package_analysis if needed
            container_id = None
            if hasattr(results, 'get') and isinstance(results, dict):
                container_id = results.get('container_id')
            
            # Update heartbeat during processing
            with transaction.atomic():
                task.refresh_from_db()
                if task.status == 'running':  # Only update if still running
                    task.last_heartbeat = timezone.now()
                    if container_id:
                        task.container_id = container_id
                    task.save()
            
        except Exception as analysis_error:
            logger.error(f"❌ Analysis failed for task {task_id}: {str(analysis_error)}")
            raise
        
        # Calculate duration
        end_time = timezone.now()
        duration = (end_time - start_time).total_seconds()
        
        # Save to cache (7 days)
        cache.set(cache_key, results, timeout=7*24*60*60)
        
        # Save results to database
        from .views import save_report, save_professional_report
        
        # Save report using existing helper
        save_report(results)
        from .models import ReportDynamicAnalysis
        latest_report = ReportDynamicAnalysis.objects.latest('id')
        
        with transaction.atomic():
            task.status = 'completed'
            task.completed_at = end_time
            task.duration_seconds = duration
            task.report = latest_report
            task.save()
        
        # Save professional report (downloadable JSON)
        try:
            # Create minimal request-like object for save_professional_report
            class MockRequest:
                def build_absolute_uri(self, url):
                    base_url = getattr(settings, 'BASE_URL', 'http://localhost:8000')
                    return f"{base_url}{url}"
            
            mock_request = MockRequest()
            download_url, report_metadata = save_professional_report(task, mock_request)
            
            with transaction.atomic():
                task.download_url = download_url
                task.save()
            
            logger.info(f"✅ Task {task_id} completed in {duration:.2f}s. Download URL: {download_url}")
            
        except Exception as save_error:
            logger.warning(f"Failed to save professional report for task {task_id}: {save_error}")
        
        # Process next queued task
        _process_next_queued_task()
        
        return {
            'status': 'success',
            'task_id': task_id,
            'duration': duration,
            'cached': False
        }
        
    except Retry:
        # Re-raise retry exceptions
        raise
    except Exception as e:
        logger.error(f"❌ Task {task_id} failed: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Update task status
        try:
            with transaction.atomic():
                task = AnalysisTask.objects.get(id=task_id)
                task.status = 'failed'
                task.completed_at = timezone.now()
                task.error_message = str(e)
                error_category = 'unknown_error'
                error_details = {}
                
                # Check if this is our custom AnalysisError with detailed information
                if hasattr(e, 'error_details'):
                    error_details = e.error_details
                    error_category = error_details.get('error_category', 'unknown_error')
                
                task.error_category = error_category
                task.error_details = error_details
                task.queue_position = None
                task.save()
        except Exception as save_error:
            logger.error(f"Failed to save error state: {save_error}")
        
        # Process next queued task even on failure
        _process_next_queued_task()
        
        # Retry with exponential backoff
        if self.request.retries < self.max_retries:
            retry_countdown = 60 * (2 ** self.request.retries)
            logger.info(f"🔄 Retrying task {task_id} in {retry_countdown}s (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(exc=e, countdown=retry_countdown)
        else:
            logger.error(f"💀 Task {task_id} failed permanently after {self.max_retries} retries")
            raise


def _process_next_queued_task():
    """
    Helper function to process the next queued task.
    Called after a task completes or fails to continue processing the queue.
    """
    try:
        with transaction.atomic():
            # Check if there's already a running task
            running_task = AnalysisTask.objects.filter(status='running').first()
            if running_task:
                return  # Another task is running, don't start new one
            
            # Get the next queued task (highest priority, then oldest)
            next_task = AnalysisTask.objects.filter(
                status='queued'
            ).order_by('-priority', 'queued_at').first()
            
            if next_task:
                # Check for existing completed result
                if next_task.purl:
                    completed_task = AnalysisTask.objects.filter(
                        purl=next_task.purl,
                        status='completed',
                        report__isnull=False
                    ).exclude(id=next_task.id).order_by('-completed_at').first()
                    
                    if completed_task:
                        logger.info(f"Task {next_task.id} already has completed result, marking as completed")
                        next_task.status = 'completed'
                        next_task.completed_at = timezone.now()
                        next_task.report = completed_task.report
                        next_task.download_url = completed_task.download_url
                        next_task.queue_position = None
                        next_task.save()
                        _update_queue_positions()
                        # Recursively process next task
                        _process_next_queued_task()
                        return
                
                # Queue the task via Celery
                logger.info(f"📤 Queuing next task {next_task.id} via Celery")
                run_dynamic_analysis.apply_async(
                    args=[next_task.id],
                    priority=next_task.priority,
                    queue='analysis'
                )
                
    except Exception as e:
        logger.error(f"Error processing next queued task: {e}")


def _update_queue_positions():
    """Update queue positions for all queued tasks."""
    try:
        with transaction.atomic():
            queued_tasks = AnalysisTask.objects.filter(
                status='queued'
            ).order_by('-priority', 'queued_at')
            
            for index, task in enumerate(queued_tasks, 1):
                task.queue_position = index
                task.save()
    except Exception as e:
        logger.error(f"Error updating queue positions: {e}")


@shared_task
def check_timeouts():
    """
    Periodic task to check for timed out analysis tasks.
    Runs every 60 seconds via Celery Beat.
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
                logger.warning(f"⏰ Task {task.id} has timed out after {task.timeout_minutes} minutes")
                
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
            
            if timed_out_tasks:
                logger.info(f"⏰ Handled {len(timed_out_tasks)} timed out tasks")
                # Process next queued task after timeout
                _process_next_queued_task()
            
            return {
                'timed_out_count': len(timed_out_tasks),
                'checked_at': timezone.now().isoformat()
            }
            
    except Exception as e:
        logger.error(f"Error checking timeouts: {e}")
        return {'error': str(e)}


@shared_task
def cleanup_old_tasks():
    """
    Periodic task to clean up old completed/failed tasks.
    Keeps tasks for 7 days.
    Runs every hour via Celery Beat.
    """
    cutoff_date = timezone.now() - timedelta(days=7)
    
    # Delete old completed tasks
    deleted_completed = AnalysisTask.objects.filter(
        status='completed',
        completed_at__lt=cutoff_date
    ).delete()[0]
    
    # Delete old failed tasks
    deleted_failed = AnalysisTask.objects.filter(
        status='failed',
        completed_at__lt=cutoff_date
    ).delete()[0]
    
    total_deleted = deleted_completed + deleted_failed
    
    logger.info(f"🧹 Cleaned up {total_deleted} old tasks ({deleted_completed} completed, {deleted_failed} failed)")
    
    return {
        'deleted_completed': deleted_completed,
        'deleted_failed': deleted_failed,
        'total': total_deleted
    }


@shared_task
def test_task():
    """Simple test task to verify Celery is working"""
    logger.info("✅ Celery is working!")
    return "Celery is working!"
