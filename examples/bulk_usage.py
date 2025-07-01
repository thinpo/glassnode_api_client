#!/usr/bin/env python3
"""
Glassnode API Client - Bulk Metrics Usage Examples

This script demonstrates how to use the Glassnode Python client
to fetch bulk data for multiple assets efficiently.
"""

import os
import sys
from datetime import datetime, timedelta

# Add parent directory to path to import glassnode_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from glassnode_client import GlassnodeClient, GlassnodeAPIError


def main():
    """Bulk usage examples for the Glassnode API client"""
    
    # Initialize the client
    try:
        client = GlassnodeClient()
        print("✅ Glassnode client initialized successfully")
    except ValueError as e:
        print(f"❌ Error: {e}")
        print("Please set your API key using: export GLASSNODE_API_KEY=your-key-here")
        return
    
    # Define assets and date range for examples
    assets = ["BTC", "ETH", "LTC", "ADA"]
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)  # Last 7 days (within bulk constraints)
    
    print(f"\n📊 Fetching bulk data for {assets} from {start_date.date()} to {end_date.date()}")
    
    try:
        # Example 1: Bulk Price Data for Multiple Assets
        print("\n1️⃣ Bulk Price Data (Multiple Assets)")
        bulk_prices = client.market.price_bulk(
            assets=assets,
            since=start_date,
            until=end_date,
            interval="24h"
        )
        print(f"Fetched data for {len(bulk_prices['a'].unique())} assets")
        print(f"Total data points: {len(bulk_prices)}")
        print("\nSample data:")
        print(bulk_prices.head())
        
        # Show price comparison
        print("\nLatest prices by asset:")
        latest_prices = bulk_prices.groupby('a')['v'].last()
        for asset, price in latest_prices.items():
            print(f"  {asset}: ${price:,.2f}")
        
        # Example 2: Bulk Market Cap Data
        print("\n2️⃣ Bulk Market Cap Data")
        bulk_mcap = client.market.market_cap_bulk(
            assets=["BTC", "ETH"],
            since=start_date,
            until=end_date,
            interval="24h"
        )
        print(f"Market cap data points: {len(bulk_mcap)}")
        
        # Calculate market cap ratio
        mcap_pivot = bulk_mcap.pivot_table(index=bulk_mcap.index, columns='a', values='v')
        if 'BTC' in mcap_pivot.columns and 'ETH' in mcap_pivot.columns:
            btc_eth_ratio = mcap_pivot['BTC'] / mcap_pivot['ETH']
            print(f"Current BTC/ETH market cap ratio: {btc_eth_ratio.iloc[-1]:.2f}")
        
        # Example 3: Bulk Active Addresses
        print("\n3️⃣ Bulk Active Addresses")
        bulk_addresses = client.addresses.active_count_bulk(
            assets=assets[:2],  # Just BTC and ETH for this example
            since=start_date,
            until=end_date,
            interval="24h"
        )
        print(f"Active addresses data points: {len(bulk_addresses)}")
        
        # Show address activity comparison
        print("\nAverage daily active addresses:")
        avg_addresses = bulk_addresses.groupby('a')['v'].mean()
        for asset, count in avg_addresses.items():
            print(f"  {asset}: {count:,.0f}")
        
        # Example 4: Bulk MVRV Ratio
        print("\n4️⃣ Bulk MVRV Ratio")
        bulk_mvrv = client.market.mvrv_bulk(
            assets=["BTC", "ETH"],
            since=start_date,
            until=end_date,
            interval="24h"
        )
        print(f"MVRV data points: {len(bulk_mvrv)}")
        
        # Show current MVRV levels
        print("\nCurrent MVRV levels:")
        current_mvrv = bulk_mvrv.groupby('a')['v'].last()
        for asset, mvrv in current_mvrv.items():
            status = "Overbought" if mvrv > 3.7 else "Oversold" if mvrv < 1.0 else "Neutral"
            print(f"  {asset}: {mvrv:.3f} ({status})")
        
    except GlassnodeAPIError as e:
        print(f"❌ API Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def advanced_bulk_analysis():
    """Advanced bulk analysis examples"""
    
    try:
        client = GlassnodeClient()
        
        print("\n🔍 Advanced Bulk Analysis")
        
        # Multi-asset correlation analysis
        print("\n📈 Multi-Asset Price Correlation (Last 30 days)")
        assets = ["BTC", "ETH", "ADA", "DOT"]
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        # Fetch bulk price data
        bulk_prices = client.market.price_bulk(
            assets=assets,
            since=start_date,
            until=end_date,
            interval="24h"
        )
        
        # Pivot data for correlation analysis
        price_pivot = bulk_prices.pivot_table(
            index=bulk_prices.index, 
            columns='a', 
            values='v'
        )
        
        # Calculate daily returns
        returns = price_pivot.pct_change().dropna()
        
        # Show correlation matrix
        correlation_matrix = returns.corr()
        print("\nPrice correlation matrix:")
        print(correlation_matrix.round(3))
        
        # Find most and least correlated pairs
        correlations = []
        for i, asset1 in enumerate(assets):
            for j, asset2 in enumerate(assets):
                if i < j and asset1 in correlation_matrix.index and asset2 in correlation_matrix.columns:
                    corr = correlation_matrix.loc[asset1, asset2]
                    correlations.append((asset1, asset2, corr))
        
        correlations.sort(key=lambda x: x[2], reverse=True)
        print(f"\nMost correlated pair: {correlations[0][0]}-{correlations[0][1]} ({correlations[0][2]:.3f})")
        print(f"Least correlated pair: {correlations[-1][0]}-{correlations[-1][1]} ({correlations[-1][2]:.3f})")
        
        # Portfolio diversification insight
        avg_correlation = sum(corr[2] for corr in correlations) / len(correlations)
        print(f"Average correlation: {avg_correlation:.3f}")
        
        if avg_correlation > 0.8:
            print("⚠️  High correlation - limited diversification benefit")
        elif avg_correlation > 0.5:
            print("📊 Moderate correlation - some diversification benefit")
        else:
            print("✅ Low correlation - good diversification potential")
        
    except GlassnodeAPIError as e:
        print(f"❌ API Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def bulk_vs_individual_comparison():
    """Compare bulk vs individual API calls"""
    
    try:
        client = GlassnodeClient()
        
        print("\n⚡ Bulk vs Individual API Calls Comparison")
        
        assets = ["BTC", "ETH"]
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        # Simulate individual calls (don't actually make them to save credits)
        print(f"\nIndividual approach would require:")
        print(f"  • {len(assets)} separate API calls")
        print(f"  • {len(assets)} API credits")
        print(f"  • Multiple requests and response parsing")
        
        # Single bulk call
        print(f"\nBulk approach:")
        print(f"  • 1 API call")
        print(f"  • {len(assets)} API credits (same total cost)")
        print(f"  • Single request with all data")
        
        # Demonstrate the bulk call
        bulk_data = client.market.price_bulk(
            assets=assets,
            since=start_date,
            until=end_date,
            interval="24h"
        )
        
        print(f"\n✅ Bulk call returned {len(bulk_data)} data points")
        print(f"Data structure includes asset identifier ('a' column)")
        print("\nBenefits of bulk approach:")
        print("  • Reduced network overhead")
        print("  • Simplified data management")
        print("  • Better for systematic analysis")
        print("  • Consistent timestamps across assets")
        
    except GlassnodeAPIError as e:
        print(f"❌ API Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def demonstrate_constraints():
    """Demonstrate bulk API constraints and best practices"""
    
    print("\n📋 Bulk API Constraints and Best Practices")
    
    print("\n⏰ Timerange Constraints:")
    print("  • 10m/1h intervals: max 10 days")
    print("  • 24h interval: max 31 days")
    print("  • 1w/1month intervals: max 93 days")
    
    print("\n💰 API Credit Usage:")
    print("  • Credits = assets × other_parameters")
    print("  • Example: 5 assets × 3 exchanges = 15 credits")
    print("  • Always specify parameter filters to control costs")
    
    print("\n✅ Best Practices:")
    print("  • Use bulk for systematic multi-asset analysis")
    print("  • Filter parameters to control credit usage")
    print("  • Respect timerange constraints")
    print("  • Check bulk_supported in metadata before use")
    print("  • Use appropriate intervals for your analysis")
    
    print("\n🚀 Ideal Use Cases:")
    print("  • Multi-asset portfolio analysis")
    print("  • Cross-asset correlation studies")
    print("  • Market comparison research")
    print("  • Systematic data collection")


if __name__ == "__main__":
    main()
    advanced_bulk_analysis()
    bulk_vs_individual_comparison()
    demonstrate_constraints() 