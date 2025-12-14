import os
from celery import Celery

# Set default Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'packamal.settings')

app = Celery('packamal')

# Load config from Django settings with CELERY_ namespace
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks in all installed apps
app.autodiscover_tasks()

# Configuration
app.conf.update(
    # Worker settings
    worker_prefetch_multiplier=1,  # Process one task at a time per worker
    worker_max_tasks_per_child=50,  # Restart worker after 50 tasks (memory cleanup)
    
    # Task acknowledgment
    task_acks_late=True,  # Acknowledge task only after completion
    task_reject_on_worker_lost=True,  # Re-queue if worker crashes
    
    # Time limits
    task_time_limit=900,  # 15 minutes hard limit
    task_soft_time_limit=840,  # 14 minutes soft limit (warning)
    
    # Result backend
    result_expires=3600,  # Results expire after 1 hour
    
    # Task routing
    task_routes={
        'package_analysis.tasks.run_dynamic_analysis': {'queue': 'analysis'},
        'package_analysis.tasks.cleanup_old_tasks': {'queue': 'maintenance'},
    },
)

@app.task(bind=True)
def debug_task(self):
    """Debug task to test Celery is working"""
    print(f'Request: {self.request!r}')
