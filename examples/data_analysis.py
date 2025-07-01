#!/usr/bin/env python3
"""
Glassnode API Client - Advanced Data Analysis Examples

This script demonstrates advanced usage of the Glassnode Python client
for cryptocurrency data analysis and visualization.
"""

import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Add parent directory to path to import glassnode_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from glassnode_client import GlassnodeClient, GlassnodeAPIError

# Optional: matplotlib for plotting (install with: pip install matplotlib)
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    PLOTTING_AVAILABLE = True
except ImportError:
    PLOTTING_AVAILABLE = False
    print("📝 Note: Install matplotlib for plotting functionality: pip install matplotlib")


class GlassnodeAnalyzer:
    """Advanced analyzer for Glassnode data"""
    
    def __init__(self, api_key: str = None):
        """Initialize the analyzer with Glassnode client"""
        self.client = GlassnodeClient(api_key=api_key)
    
    def fetch_btc_market_data(self, days: int = 365) -> pd.DataFrame:
        """Fetch comprehensive Bitcoin market data"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        print(f"📊 Fetching {days} days of Bitcoin data...")
        
        try:
            # Fetch multiple metrics
            price = self.client.market.price("BTC", since=start_date, interval="24h")
            market_cap = self.client.market.market_cap("BTC", since=start_date, interval="24h")
            mvrv = self.client.market.mvrv("BTC", since=start_date, interval="24h")
            realized_cap = self.client.market.realized_cap("BTC", since=start_date, interval="24h")
            active_addresses = self.client.addresses.active_count("BTC", since=start_date, interval="24h")
            tx_count = self.client.transactions.count("BTC", since=start_date, interval="24h")
            
            # Combine into a single DataFrame
            data = pd.DataFrame({
                'price': price['v'],
                'market_cap': market_cap['v'],
                'mvrv': mvrv['v'],
                'realized_cap': realized_cap['v'],
                'active_addresses': active_addresses['v'],
                'tx_count': tx_count['v']
            }, index=price.index)
            
            print(f"✅ Successfully fetched {len(data)} data points")
            return data
            
        except GlassnodeAPIError as e:
            print(f"❌ API Error: {e}")
            return pd.DataFrame()
    
    def calculate_technical_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators"""
        if data.empty:
            return data
        
        print("🔧 Calculating technical indicators...")
        
        # Price-based indicators
        data['price_ma_7'] = data['price'].rolling(window=7).mean()
        data['price_ma_30'] = data['price'].rolling(window=30).mean()
        data['price_volatility_30'] = data['price'].rolling(window=30).std()
        
        # Returns
        data['daily_return'] = data['price'].pct_change()
        data['cumulative_return'] = (1 + data['daily_return']).cumprod() - 1
        
        # MVRV levels
        data['mvrv_ma_30'] = data['mvrv'].rolling(window=30).mean()
        data['mvrv_overbought'] = data['mvrv'] > 3.7  # Historical overbought level
        data['mvrv_oversold'] = data['mvrv'] < 1.0   # Historical oversold level
        
        # Address activity momentum
        data['addresses_ma_7'] = data['active_addresses'].rolling(window=7).mean()
        data['address_momentum'] = data['active_addresses'] / data['addresses_ma_7'] - 1
        
        return data
    
    def analyze_correlations(self, data: pd.DataFrame) -> pd.DataFrame:
        """Analyze correlations between different metrics"""
        if data.empty:
            return pd.DataFrame()
        
        print("📈 Analyzing correlations...")
        
        # Select numeric columns for correlation
        numeric_cols = ['price', 'market_cap', 'mvrv', 'active_addresses', 'tx_count']
        correlation_matrix = data[numeric_cols].corr()
        
        print("\n🔗 Correlation Matrix:")
        print(correlation_matrix.round(3))
        
        return correlation_matrix
    
    def detect_market_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """Detect potential market signals based on on-chain metrics"""
        if data.empty:
            return data
        
        print("🚨 Detecting market signals...")
        
        signals = pd.DataFrame(index=data.index)
        
        # MVRV signals
        signals['mvrv_top_signal'] = (data['mvrv'] > 3.7) & (data['mvrv'].shift(1) <= 3.7)
        signals['mvrv_bottom_signal'] = (data['mvrv'] < 1.0) & (data['mvrv'].shift(1) >= 1.0)
        
        # Price momentum signals
        signals['golden_cross'] = (data['price_ma_7'] > data['price_ma_30']) & \
                                 (data['price_ma_7'].shift(1) <= data['price_ma_30'].shift(1))
        signals['death_cross'] = (data['price_ma_7'] < data['price_ma_30']) & \
                                (data['price_ma_7'].shift(1) >= data['price_ma_30'].shift(1))
        
        # Network activity signals
        signals['address_surge'] = data['address_momentum'] > 0.2  # 20% above 7-day average
        signals['address_decline'] = data['address_momentum'] < -0.2  # 20% below 7-day average
        
        return signals
    
    def generate_report(self, data: pd.DataFrame, signals: pd.DataFrame):
        """Generate a comprehensive market report"""
        if data.empty:
            return
        
        print("\n" + "="*50)
        print("📋 BITCOIN MARKET ANALYSIS REPORT")
        print("="*50)
        
        latest = data.iloc[-1]
        
        # Current metrics
        print(f"\n💰 Current Metrics (as of {data.index[-1].date()}):")
        print(f"  Price: ${latest['price']:,.2f}")
        print(f"  Market Cap: ${latest['market_cap']:,.0f}")
        print(f"  MVRV Ratio: {latest['mvrv']:.2f}")
        print(f"  Active Addresses: {latest['active_addresses']:,.0f}")
        print(f"  Daily Transactions: {latest['tx_count']:,.0f}")
        
        # Performance metrics
        period_return = (latest['price'] / data['price'].iloc[0] - 1) * 100
        volatility = data['daily_return'].std() * np.sqrt(365) * 100
        
        print(f"\n📊 Performance Metrics:")
        print(f"  Period Return: {period_return:+.1f}%")
        print(f"  Annualized Volatility: {volatility:.1f}%")
        print(f"  Max Drawdown: {((data['price'] / data['price'].cummax()) - 1).min() * 100:.1f}%")
        
        # Signal analysis
        recent_signals = signals.tail(30)  # Last 30 days
        
        print(f"\n🚨 Recent Signals (Last 30 days):")
        print(f"  MVRV Top Signals: {recent_signals['mvrv_top_signal'].sum()}")
        print(f"  MVRV Bottom Signals: {recent_signals['mvrv_bottom_signal'].sum()}")
        print(f"  Golden Crosses: {recent_signals['golden_cross'].sum()}")
        print(f"  Death Crosses: {recent_signals['death_cross'].sum()}")
        
        # Market assessment
        print(f"\n🎯 Market Assessment:")
        if latest['mvrv'] > 3.7:
            print("  ⚠️  MVRV suggests market is potentially overbought")
        elif latest['mvrv'] < 1.0:
            print("  ✅ MVRV suggests market is potentially oversold")
        else:
            print("  ➡️  MVRV is in neutral territory")
        
        if latest['price'] > latest['price_ma_30']:
            print("  📈 Price is above 30-day moving average (bullish)")
        else:
            print("  📉 Price is below 30-day moving average (bearish)")
    
    def plot_analysis(self, data: pd.DataFrame, signals: pd.DataFrame):
        """Create visualization plots"""
        if not PLOTTING_AVAILABLE or data.empty:
            print("📊 Plotting not available (install matplotlib)")
            return
        
        print("\n📊 Generating plots...")
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Bitcoin On-Chain Analysis', fontsize=16)
        
        # Price and moving averages
        axes[0, 0].plot(data.index, data['price'], label='Price', linewidth=2)
        axes[0, 0].plot(data.index, data['price_ma_7'], label='7-day MA', alpha=0.7)
        axes[0, 0].plot(data.index, data['price_ma_30'], label='30-day MA', alpha=0.7)
        axes[0, 0].set_title('Price and Moving Averages')
        axes[0, 0].set_ylabel('Price (USD)')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)
        
        # MVRV with signals
        axes[0, 1].plot(data.index, data['mvrv'], label='MVRV', color='orange', linewidth=2)
        axes[0, 1].axhline(y=3.7, color='red', linestyle='--', alpha=0.7, label='Overbought (3.7)')
        axes[0, 1].axhline(y=1.0, color='green', linestyle='--', alpha=0.7, label='Oversold (1.0)')
        
        # Mark signals
        top_signals = signals[signals['mvrv_top_signal']]
        bottom_signals = signals[signals['mvrv_bottom_signal']]
        
        if not top_signals.empty:
            axes[0, 1].scatter(top_signals.index, data.loc[top_signals.index, 'mvrv'], 
                              color='red', s=50, marker='v', label='Top Signal')
        if not bottom_signals.empty:
            axes[0, 1].scatter(bottom_signals.index, data.loc[bottom_signals.index, 'mvrv'], 
                              color='green', s=50, marker='^', label='Bottom Signal')
        
        axes[0, 1].set_title('MVRV Ratio with Signals')
        axes[0, 1].set_ylabel('MVRV')
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3)
        
        # Active addresses
        axes[1, 0].plot(data.index, data['active_addresses'], label='Active Addresses', color='purple')
        axes[1, 0].plot(data.index, data['addresses_ma_7'], label='7-day MA', alpha=0.7)
        axes[1, 0].set_title('Active Addresses')
        axes[1, 0].set_ylabel('Count')
        axes[1, 0].legend()
        axes[1, 0].grid(True, alpha=0.3)
        
        # Market cap vs realized cap
        axes[1, 1].plot(data.index, data['market_cap'], label='Market Cap', linewidth=2)
        axes[1, 1].plot(data.index, data['realized_cap'], label='Realized Cap', linewidth=2)
        axes[1, 1].set_title('Market Cap vs Realized Cap')
        axes[1, 1].set_ylabel('USD')
        axes[1, 1].legend()
        axes[1, 1].grid(True, alpha=0.3)
        
        # Format x-axis
        for ax in axes.flat:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        plt.tight_layout()
        plt.savefig('bitcoin_analysis.png', dpi=300, bbox_inches='tight')
        print("💾 Chart saved as 'bitcoin_analysis.png'")
        plt.show()


def main():
    """Main analysis function"""
    
    # Initialize analyzer
    try:
        analyzer = GlassnodeAnalyzer()
        print("✅ Glassnode analyzer initialized successfully")
    except ValueError as e:
        print(f"❌ Error: {e}")
        print("Please set your API key using: export GLASSNODE_API_KEY=your-key-here")
        return
    
    # Fetch data (adjust days as needed based on your API plan)
    data = analyzer.fetch_btc_market_data(days=180)  # 6 months
    
    if data.empty:
        print("❌ No data fetched. Check your API key and connection.")
        return
    
    # Calculate indicators
    data = analyzer.calculate_technical_indicators(data)
    
    # Analyze correlations
    correlation_matrix = analyzer.analyze_correlations(data)
    
    # Detect signals
    signals = analyzer.detect_market_signals(data)
    
    # Generate report
    analyzer.generate_report(data, signals)
    
    # Create plots
    analyzer.plot_analysis(data, signals)
    
    # Export data for further analysis
    data.to_csv('bitcoin_data.csv')
    signals.to_csv('bitcoin_signals.csv')
    print("\n💾 Data exported to 'bitcoin_data.csv' and 'bitcoin_signals.csv'")


if __name__ == "__main__":
    main() 