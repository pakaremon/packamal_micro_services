import os
from celery import Celery
from kombu import Queue

# Set default Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'packamal.settings')

app = Celery('packamal')

# Load config from Django settings with CELERY_ namespace
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks in all installed apps
app.autodiscover_tasks()

# Configuration
app.conf.update(
    # Worker settings - CRITICAL: Only 1 worker processes analysis queue to ensure single container execution
    worker_prefetch_multiplier=1,  # Process one task at a time per worker
    worker_max_tasks_per_child=50,  # Restart worker after 50 tasks (memory cleanup)
    
    # Task acknowledgment
    task_acks_late=True,  # Acknowledge task only after completion
    task_reject_on_worker_lost=True,  # Re-queue if worker crashes
    
    # Time limits - Extended for long-running analysis tasks
    task_time_limit=1800,  # 30 minutes hard limit (matches timeout_minutes default)
    task_soft_time_limit=1740,  # 29 minutes soft limit (warning)
    
    # Result backend
    result_expires=3600,  # Results expire after 1 hour
    
    # Task routing - Analysis queue for single-container execution, maintenance for cleanup
    task_routes={
        'package_analysis.tasks.run_dynamic_analysis': {'queue': 'analysis'},
        'package_analysis.tasks.check_timeouts': {'queue': 'maintenance'},
        'package_analysis.tasks.cleanup_old_tasks': {'queue': 'maintenance'},
    },
    
    # Queue definitions with priority support
    task_queues=(
        Queue('analysis', routing_key='analysis', queue_arguments={'x-max-priority': 10}),
        Queue('maintenance', routing_key='maintenance'),
        Queue('celery', routing_key='celery'),  # Default queue
    ),
    
    # Priority support (0-10, higher = more priority)
    task_default_priority=0,
    task_inherit_parent_priority=True,
    
    # Task serialization
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    
    # Beat schedule for periodic tasks
    beat_schedule={
        'check-timeouts': {
            'task': 'package_analysis.tasks.check_timeouts',
            'schedule': 60.0,  # Every 60 seconds
        },
        'cleanup-old-tasks': {
            'task': 'package_analysis.tasks.cleanup_old_tasks',
            'schedule': 3600.0,  # Every hour
        },
    },
)

@app.task(bind=True)
def debug_task(self):
    """Debug task to test Celery is working"""
    print(f'Request: {self.request!r}')
