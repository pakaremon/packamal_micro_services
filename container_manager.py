"""
Container management utilities for dynamic analysis tasks.
Handles Docker container lifecycle, monitoring, and cleanup.
"""

import subprocess
import logging
import time
from typing import Optional, List, Dict
from django.utils import timezone

logger = logging.getLogger(__name__)


class ContainerManager:
    """
    Manages Docker containers for dynamic analysis tasks.
    Provides functionality to start, monitor, and stop containers.
    """
    
    @staticmethod
    def get_running_containers() -> List[Dict[str, str]]:
        """
        Get list of currently running containers.
        
        Returns:
            List of dictionaries with container information
        """
        try:
            result = subprocess.run(
                ['docker', 'ps', '--format', '{{.ID}}\t{{.Image}}\t{{.Status}}\t{{.Names}}'],
                capture_output=True,
                text=True,
                check=True
            )
            
            containers = []
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    parts = line.split('\t')
                    if len(parts) >= 4:
                        containers.append({
                            'id': parts[0],
                            'image': parts[1],
                            'status': parts[2],
                            'name': parts[3]
                        })
            
            return containers
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get running containers: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error getting containers: {e}")
            return []
    
    @staticmethod
    def stop_container(container_id: str, timeout: int = 10) -> bool:
        """
        Stop a Docker container.
        
        Args:
            container_id: ID of the container to stop
            timeout: Timeout in seconds for graceful shutdown
            
        Returns:
            True if container was stopped successfully, False otherwise
        """
        try:
            logger.info(f"Stopping container {container_id}")
            
            # First try graceful stop
            result = subprocess.run(
                ['docker', 'stop', '--time', str(timeout), container_id],
                capture_output=True,
                text=True,
                check=True
            )
            
            logger.info(f"Container {container_id} stopped gracefully")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.warning(f"Graceful stop failed for container {container_id}: {e}")
            
            try:
                # Force kill if graceful stop failed
                logger.info(f"Force killing container {container_id}")
                result = subprocess.run(
                    ['docker', 'kill', container_id],
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                logger.info(f"Container {container_id} force killed")
                return True
                
            except subprocess.CalledProcessError as kill_error:
                logger.error(f"Failed to kill container {container_id}: {kill_error}")
                return False
                
        except Exception as e:
            logger.error(f"Unexpected error stopping container {container_id}: {e}")
            return False
    
    @staticmethod
    def remove_container(container_id: str, force: bool = False) -> bool:
        """
        Remove a Docker container.
        
        Args:
            container_id: ID of the container to remove
            force: Force removal even if container is running
            
        Returns:
            True if container was removed successfully, False otherwise
        """
        try:
            cmd = ['docker', 'rm']
            if force:
                cmd.append('-f')
            cmd.append(container_id)
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            logger.info(f"Container {container_id} removed successfully")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to remove container {container_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error removing container {container_id}: {e}")
            return False
    
    @staticmethod
    def get_container_info(container_id: str) -> Optional[Dict[str, str]]:
        """
        Get detailed information about a specific container.
        
        Args:
            container_id: ID of the container
            
        Returns:
            Dictionary with container information or None if not found
        """
        try:
            result = subprocess.run(
                ['docker', 'inspect', container_id],
                capture_output=True,
                text=True,
                check=True
            )
            
            import json
            container_data = json.loads(result.stdout)[0]
            
            return {
                'id': container_data['Id'],
                'name': container_data['Name'].lstrip('/'),
                'image': container_data['Config']['Image'],
                'status': container_data['State']['Status'],
                'started_at': container_data['State']['StartedAt'],
                'finished_at': container_data['State']['FinishedAt'],
                'exit_code': container_data['State']['ExitCode'],
                'running': container_data['State']['Running'],
                'paused': container_data['State']['Paused'],
                'restarting': container_data['State']['Restarting'],
            }
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get container info for {container_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting container info for {container_id}: {e}")
            return None
    
    @staticmethod
    def is_container_running(container_id: str) -> bool:
        """
        Check if a container is currently running.
        
        Args:
            container_id: ID of the container to check
            
        Returns:
            True if container is running, False otherwise
        """
        container_info = ContainerManager.get_container_info(container_id)
        return container_info is not None and container_info.get('running', False)
    
    @staticmethod
    def cleanup_stopped_containers() -> int:
        """
        Clean up all stopped containers.
        
        Returns:
            Number of containers cleaned up
        """
        try:
            result = subprocess.run(
                ['docker', 'container', 'prune', '-f'],
                capture_output=True,
                text=True,
                check=True
            )
            
            # Parse the output to get number of containers removed
            output = result.stdout
            if 'deleted' in output:
                # Extract number from output like "Deleted Containers: 3"
                import re
                match = re.search(r'Deleted Containers: (\d+)', output)
                if match:
                    count = int(match.group(1))
                    logger.info(f"Cleaned up {count} stopped containers")
                    return count
            
            logger.info("Container cleanup completed")
            return 0
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to cleanup containers: {e}")
            return 0
        except Exception as e:
            logger.error(f"Unexpected error during container cleanup: {e}")
            return 0
    
    @staticmethod
    def get_container_logs(container_id: str, tail: int = 100) -> str:
        """
        Get logs from a container.
        
        Args:
            container_id: ID of the container
            tail: Number of lines to retrieve from the end
            
        Returns:
            Container logs as string
        """
        try:
            result = subprocess.run(
                ['docker', 'logs', '--tail', str(tail), container_id],
                capture_output=True,
                text=True,
                check=True
            )
            
            return result.stdout
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get logs for container {container_id}: {e}")
            return f"Error retrieving logs: {e}"
        except Exception as e:
            logger.error(f"Unexpected error getting logs for container {container_id}: {e}")
            return f"Error retrieving logs: {e}"
    
    @staticmethod
    def extract_container_id_from_command(command: str) -> Optional[str]:
        """
        Extract container ID from a Docker command or output.
        
        Args:
            command: Docker command or output string
            
        Returns:
            Container ID if found, None otherwise
        """
        import re
        
        # Look for container ID patterns in the command
        patterns = [
            r'container_id=([a-f0-9]{12})',  # 12-char container ID
            r'container_id=([a-f0-9]{64})',  # Full container ID
            r'--name\s+([a-f0-9]{12})',      # Container name
            r'([a-f0-9]{12})',               # Any 12-char hex string
        ]
        
        for pattern in patterns:
            match = re.search(pattern, command)
            if match:
                return match.group(1)
        
        return None


# Global container manager instance
container_manager = ContainerManager()
