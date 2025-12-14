from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)


class PackageAnalysisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'package_analysis'
    
    def ready(self):
        """
        Initialize the queue manager when Django starts.
        This ensures the queue worker is available for processing tasks.
        """
        try:
            from .queue_manager import queue_manager
            
            # Start the queue worker if not already running
            if not queue_manager.is_worker_running():
                queue_manager._start_worker_if_needed()
                logger.info("Analysis queue worker started automatically")
            else:
                logger.info("Analysis queue worker already running")
                
        except Exception as e:
            logger.error(f"Failed to start analysis queue worker: {e}")
            # Don't raise the exception to prevent Django startup failure
