# Glassnode API Python Client

A comprehensive Python client for interacting with the [Glassnode API](https://docs.glassnode.com/basic-api/endpoints), providing access to on-chain and market data for cryptocurrencies.

## Features

- ✅ **Complete API Coverage**: 14+ endpoint categories covering all major Glassnode metrics
- ✅ **Bulk Metrics Support**: Efficient multi-asset data fetching with bulk endpoints
- ✅ **Pandas Integration**: Automatic DataFrame conversion with timestamp indexing
- ✅ **Environment Variable Support**: Secure API key management with dotenv
- ✅ **Flexible Authentication**: API key via parameter, environment variable, or .env file
- ✅ **Comprehensive Examples**: Basic usage, advanced analysis, and bulk operations
- ✅ **Error Handling**: Custom exceptions and validation
- ✅ **Type Hints**: Full typing support for better development experience

## Installation

```bash
pip install -r requirements.txt
```

### Dependencies

- `requests>=2.25.0` - HTTP requests
- `pandas>=1.3.0` - Data manipulation and analysis
- `python-dateutil>=2.8.0` - Date parsing utilities
- `python-dotenv>=0.19.0` - Environment variable management

## Quick Start

### 1. Get Your API Key

1. Sign up at [Glassnode](https://glassnode.com/)
2. Navigate to your account settings
3. Generate an API key

### 2. Set Up Environment Variables

**Option A: Using .env file (Recommended)**
```bash
# Copy the example file
cp .env.example .env

# Edit .env and add your API key
# GLASSNODE_API_KEY=your-actual-api-key-here
```

**Option B: Environment Variable**
```bash
export GLASSNODE_API_KEY=your-api-key-here
```

**Option C: Direct Parameter**
```python
client = GlassnodeClient(api_key="your-api-key-here")
```

### 3. Basic Usage

```python
from glassnode_client import GlassnodeClient

# Initialize client (automatically loads .env file)
client = GlassnodeClient()

# Get Bitcoin price for the last 30 days
btc_price = client.market.price(
    asset="BTC", 
    since="2024-01-01",
    interval="24h"
)

print(btc_price.head())
```

## API Endpoints

### Market Data
```python
# Price and market metrics
btc_price = client.market.price(asset="BTC")
market_cap = client.market.market_cap(asset="BTC")
mvrv = client.market.mvrv(asset="BTC")
realized_cap = client.market.realized_cap(asset="BTC")
```

### Addresses
```python
# Address activity metrics
active_addresses = client.addresses.active_count(asset="BTC")
total_addresses = client.addresses.count(asset="BTC")
new_addresses = client.addresses.new_non_zero_count(asset="BTC")
```

### On-Chain Indicators
```python
# Technical indicators
sopr = client.indicators.sopr(asset="BTC")
nupl = client.indicators.nupl(asset="BTC")
puell_multiple = client.indicators.puell_multiple(asset="BTC")
```

### Supply Metrics
```python
# Supply analysis
current_supply = client.supply.current(asset="BTC")
active_supply = client.supply.active_24h(asset="BTC")
illiquid_supply = client.supply.illiquid(asset="BTC")
```

## Bulk Operations (New!)

Fetch data for multiple assets efficiently with bulk endpoints:

```python
# Bulk price data for multiple assets
bulk_prices = client.market.price_bulk(
    assets=["BTC", "ETH", "LTC", "ADA"],
    since="2024-01-01",
    until="2024-01-07",
    interval="24h"
)

# Bulk active addresses
bulk_addresses = client.addresses.active_count_bulk(
    assets=["BTC", "ETH"],
    since="2024-01-01"
)

# Multi-asset correlation analysis
price_pivot = bulk_prices.pivot_table(
    index=bulk_prices.index, 
    columns='a', 
    values='v'
)
correlation_matrix = price_pivot.pct_change().corr()
```

### Bulk Benefits
- **Single API call** for multiple assets
- **Same total cost** as individual calls
- **Consistent timestamps** across assets
- **Perfect for analysis** and research

## Examples

### Basic Usage
```bash
python examples/basic_usage.py
```
Demonstrates basic functionality including individual metrics and a quick bulk example.

### Advanced Analysis
```bash
python examples/data_analysis.py
```
Shows sophisticated analysis including technical indicators, correlation studies, market signals, and visualization.

### Bulk Operations
```bash
python examples/bulk_usage.py
```
Comprehensive guide to bulk endpoints including multi-asset analysis, correlation studies, and best practices.

## API Credit Management

- **Individual calls**: 1 credit per request
- **Bulk calls**: 1 credit per asset/parameter combination
- **Bulk constraints**: Timerange limits based on interval
  - 10m/1h: max 10 days
  - 24h: max 31 days
  - 1w/1month: max 93 days

## Error Handling

```python
from glassnode_client import GlassnodeClient, GlassnodeAPIError

try:
    client = GlassnodeClient()
    data = client.market.price(asset="BTC")
except GlassnodeAPIError as e:
    print(f"API Error: {e}")
except ValueError as e:
    print(f"Configuration Error: {e}")
```

## Advanced Configuration

```python
# Custom configuration
client = GlassnodeClient(
    api_key="your-key",
    base_url="https://api.glassnode.com/v1",
    timeout=60
)

# With custom parameters
data = client.get_data(
    endpoint="metrics/market/price_usd_close",
    asset="BTC",
    since="2024-01-01",
    until="2024-01-31",
    interval="1h",
    currency="USD"
)
```

## Data Analysis Example

```python
from datetime import datetime, timedelta
import pandas as pd

# Fetch and analyze Bitcoin data
end_date = datetime.now()
start_date = end_date - timedelta(days=180)

# Get multiple metrics
price = client.market.price("BTC", since=start_date, interval="24h")
addresses = client.addresses.active_count("BTC", since=start_date, interval="24h")
mvrv = client.market.mvrv("BTC", since=start_date, interval="24h")

# Combine and analyze
data = pd.DataFrame({
    'price': price['v'],
    'addresses': addresses['v'],
    'mvrv': mvrv['v']
})

# Calculate correlations
correlation_matrix = data.corr()
print(correlation_matrix)
```

## Available Endpoints

| Category | Endpoints | Bulk Support |
|----------|-----------|--------------|
| **Market** | price, market_cap, mvrv, realized_cap, etc. | ✅ |
| **Addresses** | active_count, count, new_non_zero_count, etc. | ✅ |
| **Blockchain** | block_height, block_interval, utxo_count | 🔄 |
| **Indicators** | sopr, nupl, puell_multiple, nvt | 🔄 |
| **Supply** | current, active_24h, illiquid, liquid | 🔄 |
| **Transactions** | count, volume, transfers | 🔄 |
| **Fees** | volume_usd, gas_price_mean | 🔄 |
| **Mining** | difficulty, hash_rate, revenue | 🔄 |
| **Distribution** | balance_exchanges, holders | 🔄 |
| **Entities** | active_count | 🔄 |
| **DeFi** | total_value_locked_usd | 🔄 |
| **Derivatives** | futures_open_interest | 🔄 |
| **Institutions** | purpose_etf_holdings | 🔄 |
| **Mempool** | count, size_bytes | 🔄 |

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

Apache 2.0 License

## Support

- **Documentation**: [Glassnode API Docs](https://docs.glassnode.com/basic-api/endpoints)
- **Bulk Metrics**: [Bulk API Docs](https://docs.glassnode.com/basic-api/bulk-metrics)
- **Issues**: [GitHub Issues](https://github.com/thinpo/glassnode_api_client/issues)

## Changelog

### Latest Updates
- ✅ Added comprehensive bulk metrics support
- ✅ Added dotenv support for environment variables
- ✅ Enhanced error handling and validation
- ✅ Added timerange constraints for bulk operations
- ✅ Comprehensive examples and documentation 