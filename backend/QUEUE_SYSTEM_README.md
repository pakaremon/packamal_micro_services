# Dynamic Analysis Queue System

This document describes the new queue system implemented for dynamic analysis tasks to ensure only one container runs at a time.

## Overview

The queue system manages analysis tasks to prevent resource conflicts and ensure proper container management. When a new analysis request is submitted, it is added to a queue instead of running immediately. Only one analysis container runs at a time, with queued tasks waiting for their turn.

## Key Features

- **Single Container Execution**: Only one dynamic analysis container runs at a time
- **Smart Caching**: Always returns existing completed results instead of re-analyzing
- **Queue Management**: Tasks are queued and processed in order (with priority support)
- **Automatic Worker**: Background worker automatically processes queued tasks
- **Status Tracking**: Real-time queue position and status information
- **Priority Support**: Tasks can be assigned priority levels
- **Timeout Management**: 30-minute timeout with automatic container cleanup
- **Container Monitoring**: Real-time container status and heartbeat tracking
- **Error Handling**: Comprehensive error handling and recovery

## Database Changes

New fields added to `AnalysisTask` model:
- `queue_position`: Position in the analysis queue
- `priority`: Priority level (higher number = higher priority)
- `queued_at`: Timestamp when task was added to queue
- `timeout_minutes`: Timeout duration in minutes (default: 30)
- `container_id`: Docker container ID if running
- `last_heartbeat`: Last heartbeat timestamp from running container
- `status`: Updated to include 'queued' status

## API Changes

### 1. Analyze API (`POST /api/v1/analyze/`)

**Request Body:**
```json
{
    "purl": "pkg:pypi/requests@2.28.1",
    "priority": 0  // Optional, defaults to 0
}
```

**Response (New Request):**
```json
{
    "success": true,
    "data": {
        "task_id": 123,
        "status": "queued",
        "queue_position": 3,
        "status_url": "http://localhost:8000/api/v1/task/123/",
        "result_url": "http://localhost:8000/media/reports/pypi/requests/2.28.1.json",
        "message": "Analysis queued at position 3"
    }
}
```

**Response (Existing Completed Result):**
```json
{
    "task_id": 120,
    "status": "completed",
    "result_url": "http://localhost:8000/media/reports/pypi/requests/2.28.1.json",
    "report_metadata": {
        "filename": "2.28.1.json",
        "size_bytes": 15420,
        "created_at": "2024-01-15T10:20:00Z",
        "download_url": "http://localhost:8000/media/reports/pypi/requests/2.28.1.json",
        "folder_structure": "reports/pypi/requests/"
    },
    "message": "Analysis already exists (cached result)"
}
```

### 2. Task Status API (`GET /api/v1/task/{task_id}/`)

**Response:**
```json
{
    "success": true,
    "data": {
        "task_id": 123,
        "purl": "pkg:pypi/requests@2.28.1",
        "status": "queued",
        "queue_position": 3,
        "priority": 0,
        "queued_at": "2024-01-15T10:30:00Z",
        "created_at": "2024-01-15T10:30:00Z",
        "package_name": "requests",
        "package_version": "2.28.1",
        "ecosystem": "pypi"
    }
}
```

### 3. Queue Status API (`GET /api/v1/queue/status/`)

**Response:**
```json
{
    "success": true,
    "data": {
        "queue_length": 5,
        "running_tasks": [
            {
                "task_id": 120,
                "purl": "pkg:pypi/numpy@1.24.0",
                "started_at": "2024-01-15T10:25:00Z",
                "created_at": "2024-01-15T10:25:00Z"
            }
        ],
        "queued_tasks": [
            {
                "task_id": 121,
                "purl": "pkg:pypi/pandas@2.0.0",
                "queue_position": 1,
                "priority": 0,
                "queued_at": "2024-01-15T10:26:00Z",
                "created_at": "2024-01-15T10:26:00Z"
            }
        ]
    }
}
```

### 4. Task Queue Position API (`GET /api/v1/task/{task_id}/queue/`)

**Response:**
```json
{
    "success": true,
    "data": {
        "task_id": 123,
        "status": "queued",
        "queue_position": 3,
        "purl": "pkg:pypi/requests@2.28.1",
        "package_name": "requests",
        "package_version": "2.28.1",
        "ecosystem": "pypi"
    }
}
```

### 5. Timeout Status API (`GET /api/v1/timeout/status/`)

**Response:**
```json
{
    "success": true,
    "data": {
        "running_tasks": 1,
        "timed_out_tasks": 0,
        "tasks": [
            {
                "task_id": 123,
                "purl": "pkg:pypi/requests@2.28.1",
                "started_at": "2024-01-15T10:30:00Z",
                "timeout_minutes": 30,
                "remaining_minutes": 25,
                "is_timed_out": false,
                "container_id": "abc123def456",
                "container_running": true
            }
        ]
    }
}
```

### 6. Check Timeouts API (`POST /api/v1/timeout/check/`)

**Response:**
```json
{
    "success": true,
    "data": {
        "message": "Timeout check completed",
        "status": {
            "running_tasks": 0,
            "timed_out_tasks": 1,
            "tasks": []
        }
    }
}
```

## Task Status Flow

1. **pending**: Task created but not yet queued
2. **queued**: Task added to queue, waiting for processing
3. **running**: Task currently being processed by container
4. **completed**: Task finished successfully
5. **failed**: Task failed with error

## Smart Caching Logic

The system implements intelligent caching to prevent unnecessary re-analysis:

1. **Completed Result Check**: Before queuing or processing any task, the system first checks if a completed result already exists for the same PURL
2. **Immediate Return**: If a completed result exists, it's returned immediately without queuing or processing
3. **No Time Limit**: Completed results are cached indefinitely (no 24-hour expiration)
4. **Worker Optimization**: The background worker also checks for completed results before processing queued tasks
5. **Duplicate Prevention**: Multiple requests for the same package will always return the same cached result

## Timeout Management

The system implements comprehensive timeout management to prevent stuck containers:

1. **30-Minute Default Timeout**: All tasks have a default timeout of 30 minutes
2. **Automatic Monitoring**: Background worker checks for timed out tasks every 5 seconds
3. **Container Cleanup**: Timed out containers are automatically stopped and cleaned up
4. **Queue Continuation**: After timeout, the queue continues processing the next task
5. **Detailed Logging**: Container logs are captured before cleanup for debugging
6. **Error Classification**: Timeout errors are properly categorized and logged

### Timeout Behavior

- **Before Timeout**: Task runs normally with heartbeat updates
- **At Timeout**: Container is stopped, task marked as failed, queue continues
- **After Timeout**: Next task in queue starts processing immediately
- **Error Details**: Timeout information is stored in task error_details field

## Queue Processing

### Automatic Processing
- Queue worker starts automatically when Django application starts
- Worker runs in background thread
- Processes tasks one at a time
- Automatically handles task failures and recovery

### Manual Worker Management
You can also start the worker manually using the Django management command:

```bash
# Start worker in interactive mode
python manage.py start_queue_worker

# Start worker as daemon
python manage.py start_queue_worker --daemon

# Set log level
python manage.py start_queue_worker --log-level DEBUG
```

## Priority System

Tasks can be assigned priority levels:
- **0**: Normal priority (default)
- **1-9**: Higher priority (processed first)
- **Negative**: Lower priority (processed last)

Tasks are processed in order of:
1. Priority (higher first)
2. Queue position (earlier first)

## Error Handling

The queue system includes comprehensive error handling:
- **Container Failures**: Tasks marked as failed with error details
- **Worker Recovery**: Worker automatically restarts if it dies
- **Database Errors**: Proper transaction handling and rollback
- **Resource Management**: Prevents multiple containers from running

## Monitoring

### Queue Status Monitoring
Use the queue status API to monitor:
- Number of queued tasks
- Currently running tasks
- Queue processing rate

### Log Monitoring
The system logs important events:
- Task queuing and processing
- Worker start/stop events
- Error conditions and recovery

## Configuration

### Environment Variables
- `BASE_URL`: Base URL for generating download URLs
- `MEDIA_URL`: Media URL for serving reports
- `MEDIA_ROOT`: Directory for storing report files

### Django Settings
The queue system uses standard Django settings for:
- Database configuration
- Logging configuration
- Media file handling

## Migration

To apply the database changes:

```bash
python manage.py makemigrations package_analysis
python manage.py migrate package_analysis
```

## Testing

Test the queue system:

```python
# Test queue manager
from package_analysis.queue_manager import queue_manager

# Check if worker is running
print(queue_manager.is_worker_running())

# Get queue status
status = queue_manager.get_queue_status()
print(status)
```

## Troubleshooting

### Common Issues

1. **Worker Not Starting**
   - Check Django logs for errors
   - Ensure database migrations are applied
   - Verify no circular import issues

2. **Tasks Stuck in Queue**
   - Check if worker is running
   - Look for error messages in logs
   - Verify container resources are available

3. **Database Errors**
   - Ensure migrations are applied
   - Check database connectivity
   - Verify model field constraints

### Debug Commands

```bash
# Check queue status
python manage.py shell -c "from package_analysis.queue_manager import queue_manager; print(queue_manager.get_queue_status())"

# Check worker status
python manage.py shell -c "from package_analysis.queue_manager import queue_manager; print(queue_manager.is_worker_running())"

# List all tasks
python manage.py shell -c "from package_analysis.models import AnalysisTask; print([t.id for t in AnalysisTask.objects.all()])"
```

## Performance Considerations

- **Queue Size**: Monitor queue length to ensure timely processing
- **Resource Usage**: Single container limits resource consumption
- **Database Load**: Queue operations are optimized with proper indexing
- **Memory Usage**: Worker thread has minimal memory footprint

## Security

- **API Key Authentication**: All endpoints require valid API keys
- **Task Isolation**: Each task is processed independently
- **Error Information**: Sensitive error details are logged securely
- **Resource Limits**: Single container prevents resource exhaustion attacks
