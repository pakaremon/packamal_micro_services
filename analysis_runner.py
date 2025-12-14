"""
Analysis Runner Module

This module provides Python functionality to replace the run_analysis.sh shell script.
It handles Docker container execution for package analysis with proper volume management.
"""

import os
import subprocess
import logging
from pathlib import Path
from typing import Optional, List, Dict, Tuple

logger = logging.getLogger(__name__)


class AnalysisRunner:
    """Handles Docker-based package analysis execution."""
    
    # Docker volume names (can be overridden with env vars)
    RESULTS_VOLUME = os.getenv('RESULTS_VOLUME', 'analysis_results')
    STATIC_RESULTS_VOLUME = os.getenv('STATIC_RESULTS_VOLUME', 'analysis_static_results')
    FILE_WRITE_RESULTS_VOLUME = os.getenv('FILE_WRITE_RESULTS_VOLUME', 'analysis_write_results')
    ANALYZED_PACKAGES_VOLUME = os.getenv('ANALYZED_PACKAGES_VOLUME', 'analysis_analyzed_packages')
    LOGS_VOLUME = os.getenv('LOGS_VOLUME', 'analysis_logs')
    STRACE_LOGS_VOLUME = os.getenv('STRACE_LOGS_VOLUME', 'analysis_strace_logs')
    CONTAINER_VOLUME = os.getenv('CONTAINER_VOLUME', 'analysis_container_data')
    
    # Default analysis image
    DEFAULT_ANALYSIS_IMAGE = "docker.io/pakaremon/analysis"
    
    def __init__(self, analysis_image: Optional[str] = None):
        """
        Initialize the AnalysisRunner.
        
        Args:
            analysis_image: Docker image to use for analysis. Defaults to DEFAULT_ANALYSIS_IMAGE.
        """
        self.analysis_image = analysis_image or self.DEFAULT_ANALYSIS_IMAGE
        self._ensure_volumes()
    
    def _ensure_volumes(self) -> None:
        """Ensure all required Docker volumes exist."""
        volumes = [
            self.RESULTS_VOLUME,
            self.STATIC_RESULTS_VOLUME,
            self.FILE_WRITE_RESULTS_VOLUME,
            self.ANALYZED_PACKAGES_VOLUME,
            self.LOGS_VOLUME,
            self.STRACE_LOGS_VOLUME,
            self.CONTAINER_VOLUME,
        ]
        
        for volume in volumes:
            try:
                subprocess.run(
                    ["docker", "volume", "inspect", volume],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            except subprocess.CalledProcessError:
                # Volume doesn't exist, create it
                logger.info(f"Creating Docker volume: {volume}")
                subprocess.run(
                    ["docker", "volume", "create", volume],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
    
    def check_image_exists(self, image_name: str) -> bool:
        """
        Check if a Docker image exists locally.
        
        Args:
            image_name: Name of the Docker image to check.
            
        Returns:
            True if image exists, False otherwise.
        """
        try:
            subprocess.run(
                ["docker", "image", "inspect", image_name],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            return True
        except subprocess.CalledProcessError:
            return False
    
    def _build_docker_mounts(self, local_package_path: Optional[str] = None) -> List[str]:
        """
        Build Docker volume mount arguments.
        
        Args:
            local_package_path: Path to local package file to mount (if provided).
            
        Returns:
            List of Docker mount arguments.
        """
        mounts = [
            "-v", f"{self.CONTAINER_VOLUME}:/var/lib/containers",
            "-v", f"{self.RESULTS_VOLUME}:/results",
            "-v", f"{self.STATIC_RESULTS_VOLUME}:/staticResults",
            "-v", f"{self.FILE_WRITE_RESULTS_VOLUME}:/writeResults",
            "-v", f"{self.LOGS_VOLUME}:/tmp",
            "-v", f"{self.ANALYZED_PACKAGES_VOLUME}:/analyzedPackages",
            "-v", f"{self.STRACE_LOGS_VOLUME}:/straceLogs",
        ]
        
        if local_package_path:
            # Resolve the absolute path
            pkg_path = Path(local_package_path).resolve()
            pkg_file = pkg_path.name
            mounted_pkg_path = f"/{pkg_file}"
            mounts.extend(["-v", f"{pkg_path}:{mounted_pkg_path}"])
        
        return mounts
    
    def _build_docker_opts(
        self,
        interactive: bool = False,
        offline: bool = False,
    ) -> List[str]:
        """
        Build Docker run options.
        
        Args:
            interactive: Whether to use interactive TTY.
            offline: Whether to disable network access.
            
        Returns:
            List of Docker run options.
        """
        opts = [
            "run",
            "--cgroupns=host",
            "--privileged",
            "--rm",
            "--cpus=2.0",
            "--memory=4g",
        ]
        
        if interactive:
            opts.append("-ti")
        
        if offline:
            opts.extend(["--network", "none"])
        
        return opts
    
    def _build_analysis_args(
        self,
        ecosystem: Optional[str] = None,
        package: Optional[str] = None,
        version: Optional[str] = None,
        mode: str = "dynamic",
        local_path: Optional[str] = None,
        no_pull: bool = False,
        additional_args: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Build analysis command arguments.
        
        Args:
            ecosystem: Package ecosystem (e.g., "Rust").
            package: Package name.
            version: Package version.
            mode: Analysis mode (default: "dynamic").
            local_path: Path to local package file.
            no_pull: Whether to skip pulling the image.
            additional_args: Additional arguments to pass to analysis.
            
        Returns:
            List of analysis arguments.
        """
        args = [
            "analyze",
            "-dynamic-bucket", "file:///results/",
            "-file-writes-bucket", "file:///writeResults/",
            "-static-bucket", "file:///staticResults/",
            "-analyzed-pkg-bucket", "file:///analyzedPackages/",
            "-execution-log-bucket", "file:///results",
        ]
        
        # Add standard arguments
        if ecosystem:
            args.extend(["-ecosystem", ecosystem])
        
        if package:
            args.extend(["-package", package])
        
        if version:
            args.extend(["-version", version])
        
        if mode:
            args.extend(["-mode", mode])
        
        if local_path:
            # If local_path is provided, use the mounted path inside container
            pkg_file = Path(local_path).name
            mounted_path = f"/{pkg_file}"
            args.extend(["-local", mounted_path])
        
        if no_pull:
            args.append("-nopull")
        
        # Add any additional arguments
        if additional_args:
            args.extend(additional_args)
        
        return args
    
    def run_analysis(
        self,
        ecosystem: str,
        package: str,
        version: Optional[str] = None,
        mode: str = "dynamic",
        local_path: Optional[str] = None,
        no_pull: bool = False,
        interactive: bool = False,
        offline: bool = False,
        check_image: bool = True,
        stream_output: bool = True,
    ) -> Tuple[int, Optional[str]]:
        """
        Run package analysis in a Docker container.
        
        Args:
            ecosystem: Package ecosystem (e.g., "Rust").
            package: Package name.
            version: Package version (optional).
            mode: Analysis mode (default: "dynamic").
            local_path: Path to local package file (optional).
            no_pull: Whether to skip pulling the image.
            interactive: Whether to use interactive TTY.
            offline: Whether to disable network access.
            check_image: Whether to check if image exists before running.
            stream_output: Whether to stream output in real-time.
            
        Returns:
            Tuple of (return_code, output). If stream_output is True, output will be None.
            
        Raises:
            ValueError: If local_path is provided but file doesn't exist or isn't readable.
            subprocess.CalledProcessError: If Docker command fails.
        """
        # Validate local path if provided
        if local_path:
            pkg_path = Path(local_path)
            if not pkg_path.exists() or not pkg_path.is_file():
                raise ValueError(f"Local package path does not exist or is not a file: {local_path}")
            if not os.access(pkg_path, os.R_OK):
                raise ValueError(f"Local package path is not readable: {local_path}")
        
        # Check if image exists (if requested)
        if check_image:
            image_exists = self.check_image_exists(self.analysis_image)
            if not image_exists and no_pull:
                logger.warning(f"Image {self.analysis_image} not found locally, but -nopull was specified")
        else:
            image_exists = True
        
        # Build Docker command
        docker_opts = self._build_docker_opts(interactive=interactive, offline=offline)
        docker_mounts = self._build_docker_mounts(local_package_path=local_path)
        analysis_args = self._build_analysis_args(
            ecosystem=ecosystem,
            package=package,
            version=version,
            mode=mode,
            local_path=local_path,
            no_pull=no_pull,
        )
        
        # Combine all arguments
        docker_cmd = ["docker"] + docker_opts + docker_mounts + [self.analysis_image] + analysis_args
        
        logger.info(f"Running analysis: {' '.join(docker_cmd)}")
        
        # Execute Docker command
        if stream_output:
            # Stream output in real-time
            process = subprocess.Popen(
                docker_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                bufsize=1,
            )
            
            assert process.stdout is not None
            output_lines = []
            for line in process.stdout:
                line = line.rstrip()
                logger.info(line)
                output_lines.append(line)
            
            return_code = process.wait()
            output = "\n".join(output_lines) if output_lines else None
        else:
            # Capture all output at once
            result = subprocess.run(
                docker_cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
            )
            return_code = result.returncode
            output = result.stdout + result.stderr if result.stderr else result.stdout
        
        if return_code != 0:
            raise subprocess.CalledProcessError(
                return_code,
                docker_cmd,
                output=output,
            )
        
        return return_code, output
    
    def get_result_file(self, package_name: str) -> str:
        """
        Retrieve analysis result file from Docker volume.
        
        Args:
            package_name: Name of the package.
            
        Returns:
            JSON content of the result file as a string.
            
        Raises:
            subprocess.CalledProcessError: If Docker command fails.
        """
        result_file_name = package_name.lower() + ".json"
        docker_cmd = [
            "docker", "run", "--rm",
            "-v", f"{self.RESULTS_VOLUME}:/results",
            "alpine",
            "cat", f"/results/{result_file_name}",
        ]
        
        result = subprocess.run(
            docker_cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        
        return result.stdout


def run_package_analysis(
    package_name: str,
    package_version: Optional[str],
    ecosystem: str,
    local_path: Optional[str] = None,
    mode: str = "dynamic",
    analysis_image: Optional[str] = None,
    stream_output: bool = True,
    logger_instance: Optional[logging.Logger] = None,
) -> Dict:
    """
    Convenience function to run package analysis and return JSON results.
    
    This function replicates the functionality of the shell script call
    in helper.py's run_package_analysis method.
    
    Args:
        package_name: Name of the package to analyze.
        package_version: Version of the package (None or "latest" for latest).
        ecosystem: Package ecosystem (e.g., "Rust").
        local_path: Path to local package file (optional).
        mode: Analysis mode (default: "dynamic").
        analysis_image: Docker image to use (defaults to runner's default).
        stream_output: Whether to stream output in real-time.
        logger_instance: Logger instance to use (for backward compatibility).
        
    Returns:
        Dictionary containing:
            - elapsed_time: Time taken for analysis in seconds
            - json_data: Parsed JSON data from analysis results
        
    Raises:
        subprocess.CalledProcessError: If analysis fails.
        ValueError: If local_path is invalid.
    """
    import time
    import json
    
    log = logger_instance or logger
    
    # Determine if we should use no_pull based on image existence
    runner = AnalysisRunner(analysis_image=analysis_image)
    image_name = runner.analysis_image
    image_exists = runner.check_image_exists(image_name)
    no_pull = image_exists
    
    # Handle "latest" version
    version_arg = None if package_version == "latest" else package_version
    
    log.info(f"Running package analysis: Package={package_name}, Version={package_version}, Ecosystem={ecosystem}")
    
    # Run analysis
    start_time = time.time()
    try:
        return_code, output = runner.run_analysis(
            ecosystem=ecosystem,
            package=package_name,
            version=version_arg,
            mode=mode,
            local_path=local_path,
            no_pull=no_pull,
            interactive=False,
            offline=False,
            check_image=True,
            stream_output=stream_output,
        )
    except subprocess.CalledProcessError as e:
        log.error(f"Analysis failed with return code {e.returncode}")
        raise
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # Retrieve results
    try:
        result_json = runner.get_result_file(package_name)
        json_data = json.loads(result_json)
        
        return {
            'elapsed_time': elapsed_time,
            'json_data': json_data,
        }
        
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        log.error(f"Failed to retrieve or parse results: {e}")
        raise

