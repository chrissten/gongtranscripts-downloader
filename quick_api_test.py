#!/usr/bin/env python3
"""
Quick test to validate API endpoint differences.
"""
import asyncio
from config import load_config
from gong_client import GongAPIClient

async def quick_test():
    """Quick test of API endpoints."""
    config = load_config()
    
    print("🔍 Quick API Endpoint Test")
    print("=" * 40)
    
    async with GongAPIClient(config) as client:
        # Test basic endpoint with small date range
        print("\n📡 Testing GET /v2/calls")
        try:
            calls = await client.get_calls_list("2025-06-27", "2025-06-27")
            if calls:
                sample = calls[0]
                print(f"✅ Found {len(calls)} calls")
                print(f"📋 Keys: {list(sample.keys())}")
                print(f"👥 Has parties: {'parties' in sample}")
                print(f"📊 Has metaData: {'metaData' in sample}")
            else:
                print("❌ No calls found")
        except Exception as e:
            print(f"❌ Error: {e}")
        
        # Test extensive endpoint with same small range
        print("\n📡 Testing POST /v2/calls/extensive")
        try:
            calls = await client.get_calls_list_extensive("2025-06-27", "2025-06-27")
            if calls:
                sample = calls[0]
                print(f"✅ Found {len(calls)} calls")
                print(f"📋 Keys: {list(sample.keys())}")
                print(f"👥 Has parties: {'parties' in sample}")
                print(f"📊 Has metaData: {'metaData' in sample}")
                
                if 'parties' in sample:
                    parties = sample['parties']
                    print(f"👥 Parties count: {len(parties) if isinstance(parties, list) else 'N/A'}")
                    if isinstance(parties, list) and parties:
                        print(f"👥 First party: {parties[0]}")
            else:
                print("❌ No calls found")
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(quick_test()) 