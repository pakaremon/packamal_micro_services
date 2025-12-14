"""
Django management command to start the analysis queue worker.
This command starts a background worker that processes queued analysis tasks.
Only one container runs at a time to ensure proper resource management.
"""

from django.core.management.base import BaseCommand
from django.utils import timezone
from package_analysis.queue_manager import queue_manager
import signal
import sys
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Start the analysis queue worker to process queued tasks'

    def add_arguments(self, parser):
        parser.add_argument(
            '--daemon',
            action='store_true',
            help='Run as daemon (detached from terminal)',
        )
        parser.add_argument(
            '--log-level',
            type=str,
            default='INFO',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
            help='Set the logging level',
        )

    def handle(self, *args, **options):
        """Handle the command execution."""
        # Set up logging
        log_level = getattr(logging, options['log_level'].upper())
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        self.stdout.write(
            self.style.SUCCESS('Starting analysis queue worker...')
        )
        
        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            self.stdout.write(
                self.style.WARNING(f'Received signal {signum}, shutting down gracefully...')
            )
            queue_manager.stop_worker()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            # Start the queue manager worker
            queue_manager._start_worker_if_needed()
            
            if options['daemon']:
                self.stdout.write(
                    self.style.SUCCESS('Queue worker started as daemon')
                )
                # In daemon mode, just keep the process alive
                while True:
                    import time
                    time.sleep(60)  # Check every minute
                    if not queue_manager.is_worker_running():
                        self.stdout.write(
                            self.style.ERROR('Worker thread died, restarting...')
                        )
                        queue_manager._start_worker_if_needed()
            else:
                self.stdout.write(
                    self.style.SUCCESS('Queue worker started. Press Ctrl+C to stop.')
                )
                
                # In interactive mode, keep the process alive and show status
                import time
                while True:
                    time.sleep(30)  # Show status every 30 seconds
                    
                    if not queue_manager.is_worker_running():
                        self.stdout.write(
                            self.style.ERROR('Worker thread died, restarting...')
                        )
                        queue_manager._start_worker_if_needed()
                    
                    # Show queue status
                    try:
                        status = queue_manager.get_queue_status()
                        self.stdout.write(
                            f"Queue status: {status['queue_length']} queued, "
                            f"{status['running_tasks']} running"
                        )
                    except Exception as e:
                        self.stdout.write(
                            self.style.ERROR(f'Error getting queue status: {e}')
                        )
                        
        except KeyboardInterrupt:
            self.stdout.write(
                self.style.WARNING('Received keyboard interrupt, shutting down...')
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error starting queue worker: {e}')
            )
            raise
        finally:
            queue_manager.stop_worker()
            self.stdout.write(
                self.style.SUCCESS('Queue worker stopped')
            )

