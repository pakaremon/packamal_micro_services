#!/usr/bin/env python3
"""
Test script to demonstrate the timeout functionality.
This script shows how the system handles container timeouts and cleanup.
"""

import requests
import json
import time

# Configuration
BASE_URL = "http://localhost:8000"
API_KEY = "your-api-key-here"  # Replace with actual API key

def test_timeout_functionality():
    """Test the timeout functionality of the analysis system."""
    
    print("Testing Timeout Functionality")
    print("=" * 50)
    
    # Test 1: Check current timeout status
    print("1. Checking current timeout status...")
    timeout_status = get_timeout_status()
    if timeout_status:
        print(f"   Running tasks: {timeout_status.get('running_tasks', 0)}")
        print(f"   Timed out tasks: {timeout_status.get('timed_out_tasks', 0)}")
        
        # Show details of running tasks
        tasks = timeout_status.get('tasks', [])
        for task in tasks:
            print(f"   Task {task['task_id']}: {task['remaining_minutes']} minutes remaining")
    
    print()
    
    # Test 2: Manually trigger timeout check
    print("2. Manually triggering timeout check...")
    check_result = trigger_timeout_check()
    if check_result:
        print(f"   ✅ Timeout check completed: {check_result.get('message', 'No message')}")
        status = check_result.get('status', {})
        print(f"   Running tasks after check: {status.get('running_tasks', 0)}")
        print(f"   Timed out tasks after check: {status.get('timed_out_tasks', 0)}")
    else:
        print("   ❌ Timeout check failed")
    
    print()
    
    # Test 3: Check queue status
    print("3. Checking queue status...")
    queue_status = get_queue_status()
    if queue_status:
        print(f"   Queue length: {queue_status.get('queue_length', 0)}")
        print(f"   Running tasks: {len(queue_status.get('running_tasks', []))}")
        print(f"   Queued tasks: {len(queue_status.get('queued_tasks', []))}")
    
    print()
    print("=" * 50)
    print("Timeout functionality test completed!")

def get_timeout_status():
    """Get current timeout status."""
    url = f"{BASE_URL}/api/v1/timeout/status/"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('success'):
            return data.get('data', {})
        else:
            print(f"   ❌ Timeout status check failed: {data.get('error', 'Unknown error')}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Timeout status check failed: {e}")
        return None

def trigger_timeout_check():
    """Manually trigger timeout check."""
    url = f"{BASE_URL}/api/v1/timeout/check/"
    
    try:
        response = requests.post(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('success'):
            return data.get('data', {})
        else:
            print(f"   ❌ Timeout check failed: {data.get('error', 'Unknown error')}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Timeout check failed: {e}")
        return None

def get_queue_status():
    """Get current queue status."""
    url = f"{BASE_URL}/api/v1/queue/status/"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('success'):
            return data.get('data', {})
        else:
            print(f"   ❌ Queue status check failed: {data.get('error', 'Unknown error')}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Queue status check failed: {e}")
        return None

def monitor_running_task(task_id):
    """Monitor a specific running task for timeout behavior."""
    print(f"Monitoring task {task_id} for timeout behavior...")
    
    for i in range(10):  # Monitor for 10 iterations
        url = f"{BASE_URL}/api/v1/task/{task_id}/"
        headers = {"X-API-Key": API_KEY}
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('success'):
                task_data = data.get('data', {})
                status = task_data.get('status')
                remaining_time = task_data.get('remaining_time_minutes')
                is_timed_out = task_data.get('is_timed_out', False)
                
                print(f"   Iteration {i+1}: Status={status}, Remaining={remaining_time}min, TimedOut={is_timed_out}")
                
                if status != 'running':
                    print(f"   ✅ Task {task_id} finished with status: {status}")
                    break
                    
            else:
                print(f"   ❌ Failed to get task status: {data.get('error', 'Unknown error')}")
                break
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request failed: {e}")
            break
        
        time.sleep(30)  # Wait 30 seconds between checks

if __name__ == "__main__":
    print("Dynamic Analysis Timeout Functionality Test")
    print("==========================================")
    print()
    
    # Run basic timeout tests
    test_timeout_functionality()
    
    print()
    print("Expected behavior:")
    print("- Running tasks show remaining time")
    print("- Timed out tasks are automatically cleaned up")
    print("- Queue continues processing after timeout")
    print("- Container logs are captured before cleanup")
    print()
    print("To test with a real task:")
    print("1. Submit an analysis request")
    print("2. Use monitor_running_task(task_id) to watch timeout behavior")
    print("3. Check timeout status periodically")









