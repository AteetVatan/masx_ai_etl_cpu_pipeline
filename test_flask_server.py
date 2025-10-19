#!/usr/bin/env python3
"""
Simple test script for Flask server endpoints.
"""

import requests
import json
import time


def test_flask_endpoints():
    """Test the Flask server endpoints."""
    base_url = "http://localhost:8000"

    print("Testing Flask Server Endpoints")
    print("=" * 50)

    # Test root endpoint
    try:
        response = requests.get(f"{base_url}/")
        print(f"✅ Root endpoint: {response.status_code}")
        print(f"   Response: {response.json()}")
    except Exception as e:
        print(f"❌ Root endpoint failed: {e}")

    # Test health endpoint
    try:
        response = requests.get(f"{base_url}/health")
        print(f"✅ Health endpoint: {response.status_code}")
        if response.status_code == 200:
            print(f"   Response: {response.json()}")
    except Exception as e:
        print(f"❌ Health endpoint failed: {e}")

    # Test feed process endpoint
    try:
        payload = {"date": "2024-01-01", "trigger": "test"}
        response = requests.post(
            f"{base_url}/feed/process",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        print(f"✅ Feed process endpoint: {response.status_code}")
        if response.status_code == 200:
            print(f"   Response: {response.json()}")
    except Exception as e:
        print(f"❌ Feed process endpoint failed: {e}")

    # Test feed process flashpoint endpoint
    try:
        payload = {
            "date": "2024-01-01",
            "flashpoint_id": "test_fp_123",
            "trigger": "test",
        }
        response = requests.post(
            f"{base_url}/feed/process/flashpoint",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        print(f"✅ Feed process flashpoint endpoint: {response.status_code}")
        if response.status_code == 200:
            print(f"   Response: {response.json()}")
    except Exception as e:
        print(f"❌ Feed process flashpoint endpoint failed: {e}")

    print("=" * 50)
    print("Test completed!")


if __name__ == "__main__":
    test_flask_endpoints()
