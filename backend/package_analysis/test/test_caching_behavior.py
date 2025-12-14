#!/usr/bin/env python3
"""
Test script to demonstrate the improved caching behavior.
This script shows how the system returns cached results instead of re-analyzing.
"""

import requests
import json
import time

# Configuration
BASE_URL = "http://localhost:8000"
API_KEY = "your-api-key-here"  # Replace with actual API key

def test_caching_behavior():
    """Test the caching behavior of the analysis API."""
    
    # Test PURL - use a package that might already be analyzed
    test_purl = "pkg:pypi/requests@2.28.1"
    
    print(f"Testing caching behavior for: {test_purl}")
    print("=" * 60)
    
    # First request
    print("1. Making first request...")
    response1 = make_analysis_request(test_purl)
    print(f"   Status: {response1.get('status', 'unknown')}")
    print(f"   Message: {response1.get('message', 'no message')}")
    
    if response1.get('status') == 'completed':
        print("   ✅ First request returned cached result immediately!")
    elif response1.get('status') == 'queued':
        print("   ⏳ First request was queued (new analysis)")
    else:
        print(f"   ❓ First request status: {response1.get('status')}")
    
    print()
    
    # Wait a moment
    time.sleep(2)
    
    # Second request (should return cached result if first was completed)
    print("2. Making second request (should return cached result)...")
    response2 = make_analysis_request(test_purl)
    print(f"   Status: {response2.get('status', 'unknown')}")
    print(f"   Message: {response2.get('message', 'no message')}")
    
    if response2.get('status') == 'completed':
        print("   ✅ Second request returned cached result immediately!")
        print("   🎉 Caching behavior is working correctly!")
    elif response2.get('status') == 'queued':
        print("   ⏳ Second request was queued (no cached result found)")
    else:
        print(f"   ❓ Second request status: {response2.get('status')}")
    
    print()
    
    # Compare task IDs
    task_id1 = response1.get('task_id')
    task_id2 = response2.get('task_id')
    
    if task_id1 and task_id2:
        if task_id1 == task_id2:
            print("   ✅ Both requests returned the same task ID (cached result)")
        else:
            print(f"   📝 Different task IDs: {task_id1} vs {task_id2}")
    
    print("=" * 60)

def make_analysis_request(purl):
    """Make an analysis request to the API."""
    
    url = f"{BASE_URL}/api/v1/analyze/"
    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json"
    }
    data = {
        "purl": purl,
        "priority": 0
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Request failed: {e}")
        return {"error": str(e)}
    except json.JSONDecodeError as e:
        print(f"   ❌ JSON decode error: {e}")
        return {"error": "Invalid JSON response"}

def check_queue_status():
    """Check the current queue status."""
    
    print("Checking queue status...")
    url = f"{BASE_URL}/api/v1/queue/status/"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('success'):
            queue_data = data.get('data', {})
            print(f"   Queue length: {queue_data.get('queue_length', 0)}")
            print(f"   Running tasks: {len(queue_data.get('running_tasks', []))}")
            print(f"   Queued tasks: {len(queue_data.get('queued_tasks', []))}")
        else:
            print(f"   ❌ Queue status check failed: {data.get('error', 'Unknown error')}")
            
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Queue status check failed: {e}")

if __name__ == "__main__":
    print("Dynamic Analysis Caching Behavior Test")
    print("=====================================")
    print()
    
    # Check queue status first
    check_queue_status()
    print()
    
    # Test caching behavior
    test_caching_behavior()
    
    print()
    print("Test completed!")
    print()
    print("Expected behavior:")
    print("- If package was never analyzed: First request queued, second request queued")
    print("- If package was already analyzed: Both requests return cached result immediately")
    print("- If first request completes between calls: Second request returns cached result")










