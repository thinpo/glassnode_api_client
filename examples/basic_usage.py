#!/usr/bin/env python3
"""
Glassnode API Client - Basic Usage Examples

This script demonstrates how to use the Glassnode Python client
to fetch various cryptocurrency metrics.

Setup:
1. Copy .env.example to .env and add your API key
2. Or set GLASSNODE_API_KEY environment variable
3. Or pass api_key parameter directly to GlassnodeClient()
"""

import os
import sys
from datetime import datetime, timedelta

# Add parent directory to path to import glassnode_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from glassnode_client import GlassnodeClient, GlassnodeAPIError


def main():
    """Basic usage examples for the Glassnode API client"""
    
    # Initialize the client
    # Make sure to set your API key as an environment variable: GLASSNODE_API_KEY
    try:
        client = GlassnodeClient()
        print("✅ Glassnode client initialized successfully")
    except ValueError as e:
        print(f"❌ Error: {e}")
        print("Please set your API key using one of these methods:")
        print("  1. Create a .env file: cp .env.example .env (then add your key)")
        print("  2. Environment variable: export GLASSNODE_API_KEY=your-key-here")
        print("  3. Direct parameter: GlassnodeClient(api_key='your-key')")
        return
    
    # Define date range for examples
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)  # Last 30 days
    
    print(f"\n📊 Fetching data from {start_date.date()} to {end_date.date()}")
    
    try:
        # Example 1: Bitcoin Price
        print("\n1️⃣ Bitcoin Price (USD)")
        btc_price = client.market.price(
            asset="BTC",
            since=start_date,
            until=end_date,
            interval="24h"
        )
        print(f"Latest BTC price: ${btc_price['v'].iloc[-1]:,.2f}")
        print(f"30-day high: ${btc_price['v'].max():,.2f}")
        print(f"30-day low: ${btc_price['v'].min():,.2f}")
        
        # Example 2: Bitcoin Market Cap
        print("\n2️⃣ Bitcoin Market Cap")
        btc_mcap = client.market.market_cap(
            asset="BTC",
            since=start_date,
            until=end_date,
            interval="24h"
        )
        print(f"Current market cap: ${btc_mcap['v'].iloc[-1]:,.0f}")
        
        # Example 3: Active Addresses
        print("\n3️⃣ Bitcoin Active Addresses")
        btc_addresses = client.addresses.active_count(
            asset="BTC",
            since=start_date,
            until=end_date,
            interval="24h"
        )
        print(f"Current active addresses: {btc_addresses['v'].iloc[-1]:,.0f}")
        print(f"30-day average: {btc_addresses['v'].mean():,.0f}")
        
        # Example 4: MVRV Ratio
        print("\n4️⃣ Bitcoin MVRV Ratio")
        btc_mvrv = client.market.mvrv(
            asset="BTC",
            since=start_date,
            until=end_date,
            interval="24h"
        )
        print(f"Current MVRV: {btc_mvrv['v'].iloc[-1]:.3f}")
        
        # Example 5: Ethereum Gas Fees
        print("\n5️⃣ Ethereum Mean Gas Price")
        eth_gas = client.fees.gas_price_mean(
            asset="ETH",
            since=start_date,
            until=end_date,
            interval="24h"
        )
        print(f"Current mean gas price: {eth_gas['v'].iloc[-1]:.2f} Gwei")
        
        # Example 6: Bitcoin Mining Difficulty
        print("\n6️⃣ Bitcoin Mining Difficulty")
        btc_difficulty = client.mining.difficulty(
            asset="BTC",
            since=start_date,
            until=end_date,
            interval="24h"
        )
        print(f"Current difficulty: {btc_difficulty['v'].iloc[-1]:,.0f}")
        
        # Example 7: Transaction Count
        print("\n7️⃣ Bitcoin Transaction Count")
        btc_tx_count = client.transactions.count(
            asset="BTC",
            since=start_date,
            until=end_date,
            interval="24h"
        )
        print(f"Recent daily transactions: {btc_tx_count['v'].iloc[-1]:,.0f}")
        
    except GlassnodeAPIError as e:
        print(f"❌ API Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def metadata_examples():
    """Examples of using metadata endpoints"""
    
    try:
        client = GlassnodeClient()
        
        print("\n🔍 Metadata Examples")
        
        # Get available assets
        print("\n📋 Available Assets (first 10):")
        assets = client.metadata.assets()
        for i, asset in enumerate(assets['data'][:10]):
            print(f"  {asset['symbol']}: {asset['name']}")
        
        # Get metric metadata
        print("\n📊 MVRV Metric Metadata:")
        mvrv_meta = client.metadata.metric("/market/mvrv")
        print(f"  Supported assets: {len(mvrv_meta['parameters']['a'])} assets")
        print(f"  Supported intervals: {mvrv_meta['parameters']['i']}")
        
    except GlassnodeAPIError as e:
        print(f"❌ API Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def bulk_examples():
    """Brief introduction to bulk functionality"""
    
    try:
        client = GlassnodeClient()
        
        print("\n🚀 Bulk API Example (New Feature!)")
        print("Fetch data for multiple assets in a single request:")
        
        # Quick bulk example
        bulk_data = client.market.price_bulk(
            assets=["BTC", "ETH"],
            since="2024-01-01",
            until="2024-01-07",
            interval="24h"
        )
        
        print(f"✅ Fetched {len(bulk_data)} data points for multiple assets")
        print(f"Assets included: {bulk_data['a'].unique().tolist()}")
        print("\nBulk benefits:")
        print("  • Single API call for multiple assets")
        print("  • Consistent timestamps across assets")
        print("  • Ideal for multi-asset analysis")
        print("\n💡 See examples/bulk_usage.py for comprehensive bulk examples")
        
    except GlassnodeAPIError as e:
        print(f"❌ API Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()
    metadata_examples()
    bulk_examples() 