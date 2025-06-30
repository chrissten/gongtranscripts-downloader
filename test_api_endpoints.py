#!/usr/bin/env python3
"""
Test script to validate API endpoint differences and check participant data availability.
"""
import asyncio
import json
from config import load_config
from gong_client import GongAPIClient

async def test_api_endpoints():
    """Test both API endpoints to see which provides participant data."""
    config = load_config()
    
    print("🔍 Testing Gong API Endpoints for Participant Data")
    print("=" * 60)
    
    async with GongAPIClient(config) as client:
        # Test 1: GET /v2/calls (current endpoint)
        print("\n📡 Testing GET /v2/calls (current endpoint)")
        print("-" * 40)
        
        try:
            calls_basic = await client.get_calls_list("2025-01-01", "2025-01-02")
            if calls_basic:
                sample_call = calls_basic[0]
                print(f"✅ Found {len(calls_basic)} calls")
                print(f"📋 Sample call keys: {list(sample_call.keys())}")
                print(f"👥 Has 'parties' field: {'parties' in sample_call}")
                print(f"📊 Has 'metaData' field: {'metaData' in sample_call}")
                
                if 'parties' in sample_call:
                    parties = sample_call['parties']
                    print(f"👥 Parties type: {type(parties)}")
                    if isinstance(parties, list):
                        print(f"👥 Parties count: {len(parties)}")
                        if parties:
                            print(f"👥 First party: {parties[0]}")
            else:
                print("❌ No calls found")
        except Exception as e:
            print(f"❌ Error: {e}")
        
        # Test 2: POST /v2/calls/extensive (extensive endpoint)
        print("\n📡 Testing POST /v2/calls/extensive (extensive endpoint)")
        print("-" * 40)
        
        try:
            # Try a broader date range to find calls with participant data
            calls_extensive = await client.get_calls_list_extensive("2025-06-01", "2025-06-30")
            if calls_extensive:
                print(f"✅ Found {len(calls_extensive)} calls")
                
                # Check first few calls for participant data
                for i, sample_call in enumerate(calls_extensive[:5]):
                    print(f"\n📋 Call {i+1}:")
                    print(f"   Keys: {list(sample_call.keys())}")
                    print(f"   Has 'parties' field: {'parties' in sample_call}")
                    print(f"   Has 'metaData' field: {'metaData' in sample_call}")
                    
                    if 'parties' in sample_call:
                        parties = sample_call['parties']
                        print(f"   👥 Parties type: {type(parties)}")
                        if isinstance(parties, list):
                            print(f"   👥 Parties count: {len(parties)}")
                            if parties:
                                print(f"   👥 First party: {parties[0]}")
                    
                    if 'metaData' in sample_call:
                        meta_data = sample_call['metaData']
                        print(f"   📊 MetaData type: {type(meta_data)}")
                        if isinstance(meta_data, dict):
                            print(f"   📊 MetaData keys: {list(meta_data.keys())}")
            else:
                print("❌ No calls found")
        except Exception as e:
            print(f"❌ Error: {e}")
        
        print("\n" + "=" * 60)
        print("🎯 Summary:")
        print("- GET /v2/calls: Basic call metadata (no participants)")
        print("- POST /v2/calls/extensive: Full call data with participants")
        print("\n💡 Recommendation: Use POST /v2/calls/extensive for participant data")

if __name__ == "__main__":
    asyncio.run(test_api_endpoints()) 