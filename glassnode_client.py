"""
Glassnode API Python Client

A comprehensive Python client for interacting with the Glassnode API.
Provides access to on-chain and market data for cryptocurrencies.

Documentation: https://docs.glassnode.com/basic-api/endpoints
Bulk API: https://docs.glassnode.com/basic-api/bulk-metrics
"""

import os
import requests
import pandas as pd
from typing import Optional, Dict, Any, List, Union
from datetime import datetime
import json


class GlassnodeAPIError(Exception):
    """Custom exception for Glassnode API errors"""
    pass


class GlassnodeClient:
    """
    Glassnode API Client
    
    A comprehensive client for accessing Glassnode's cryptocurrency data API.
    
    Args:
        api_key (str, optional): Your Glassnode API key. If not provided, 
                                will look for GLASSNODE_API_KEY environment variable.
        base_url (str): Base URL for the API. Defaults to official endpoint.
        timeout (int): Request timeout in seconds. Defaults to 30.
    
    Example:
        >>> client = GlassnodeClient(api_key="your-api-key")
        >>> btc_price = client.market.price(asset="BTC", since="2024-01-01")
        >>> print(btc_price.head())
    """
    
    def __init__(
        self, 
        api_key: Optional[str] = None,
        base_url: str = "https://api.glassnode.com/v1",
        timeout: int = 30
    ):
        self.api_key = api_key or os.getenv("GLASSNODE_API_KEY")
        if not self.api_key:
            raise ValueError(
                "API key is required. Provide it as argument or set GLASSNODE_API_KEY environment variable."
            )
        
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "X-Api-Key": self.api_key,
            "User-Agent": "glassnode-python-client"
        })
        
        # Initialize endpoint categories
        self.addresses = AddressesEndpoints(self)
        self.blockchain = BlockchainEndpoints(self)
        self.market = MarketEndpoints(self)
        self.indicators = IndicatorsEndpoints(self)
        self.supply = SupplyEndpoints(self)
        self.transactions = TransactionsEndpoints(self)
        self.fees = FeesEndpoints(self)
        self.mining = MiningEndpoints(self)
        self.distribution = DistributionEndpoints(self)
        self.entities = EntitiesEndpoints(self)
        self.defi = DeFiEndpoints(self)
        self.derivatives = DerivativesEndpoints(self)
        self.institutions = InstitutionsEndpoints(self)
        self.mempool = MempoolEndpoints(self)
        self.protocols = ProtocolsEndpoints(self)
        self.signals = SignalsEndpoints(self)
        self.metadata = MetadataEndpoints(self)
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Union[List[Dict], Dict]:
        """
        Make a request to the Glassnode API
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            **kwargs: Additional parameters to add to the request
            
        Returns:
            API response data
            
        Raises:
            GlassnodeAPIError: If the API request fails
        """
        # Merge kwargs into params
        if params is None:
            params = {}
        params.update(kwargs)
        
        # Remove None values and handle list parameters for bulk endpoints
        final_params = []
        for key, value in params.items():
            if value is not None:
                if isinstance(value, list):
                    # For bulk endpoints: add multiple parameters (e.g., ?a=BTC&a=ETH)
                    for item in value:
                        final_params.append((key, item))
                else:
                    final_params.append((key, value))
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.get(url, params=final_params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise GlassnodeAPIError(f"API request failed: {e}") from e
        except json.JSONDecodeError as e:
            raise GlassnodeAPIError(f"Failed to parse JSON response: {e}") from e
    
    def get_data(
        self,
        endpoint: str,
        asset: str,
        since: Optional[Union[str, datetime, int]] = None,
        until: Optional[Union[str, datetime, int]] = None,
        interval: Optional[str] = None,
        format: str = "json",
        currency: Optional[str] = None,
        **kwargs
    ) -> Union[pd.DataFrame, List[Dict]]:
        """
        Generic method to fetch data from any endpoint
        
        Args:
            endpoint: API endpoint path
            asset: Asset symbol (e.g., "BTC", "ETH")
            since: Start date (YYYY-MM-DD, datetime, or unix timestamp)
            until: End date (YYYY-MM-DD, datetime, or unix timestamp)
            interval: Data interval (10m, 1h, 24h, 1w, 1month)
            format: Response format ("json" or "csv")
            currency: Currency for price data (e.g., "USD", "EUR")
            **kwargs: Additional endpoint-specific parameters
            
        Returns:
            DataFrame if format="json", raw response if format="csv"
        """
        params = {
            "a": asset,
            "s": self._format_timestamp(since) if since else None,
            "u": self._format_timestamp(until) if until else None,
            "i": interval,
            "f": format,
            "c": currency
        }
        params.update(kwargs)
        
        data = self._make_request(endpoint, params)
        
        if format == "json" and isinstance(data, list):
            df = pd.DataFrame(data)
            if 't' in df.columns:
                df['t'] = pd.to_datetime(df['t'], unit='s')
                df.set_index('t', inplace=True)
            return df
        
        return data
    
    def get_bulk_data(
        self,
        endpoint: str,
        assets: Optional[List[str]] = None,
        since: Optional[Union[str, datetime, int]] = None,
        until: Optional[Union[str, datetime, int]] = None,
        interval: str = "24h",
        currency: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Generic method to fetch bulk data from any endpoint (beta)
        
        Bulk endpoints allow fetching data for multiple assets or parameter combinations
        in a single request. They consume API credits equal to individual calls.
        
        Args:
            endpoint: API endpoint path
            assets: List of asset symbols (e.g., ["BTC", "ETH"]). If None, fetches all assets.
            since: Start date (YYYY-MM-DD, datetime, or unix timestamp)
            until: End date (YYYY-MM-DD, datetime, or unix timestamp)
            interval: Data interval (10m, 1h, 24h, 1w, 1month). Defaults to "24h"
            currency: Currency for price data (e.g., "USD", "EUR")
            **kwargs: Additional endpoint-specific parameters (can be lists for whitelisting)
            
        Returns:
            DataFrame with bulk data including parameter columns
            
        Raises:
            GlassnodeAPIError: If the API request fails
            
        Note:
            - Timerange constraints apply based on interval:
              * 10m/1h: max 10 days
              * 24h: max 31 days  
              * 1w/1month: max 93 days
            - Each parameter combination consumes 1 API credit
        """
        # Validate timerange constraints
        self._validate_bulk_timerange(since, until, interval)
        
        # Prepare base parameters
        params = {
            "s": self._format_timestamp(since) if since else None,
            "u": self._format_timestamp(until) if until else None,
            "i": interval,
            "f": "json",  # Only JSON supported for bulk
            "c": currency
        }
        
        # Add asset whitelist if provided
        if assets:
            params["a"] = assets
        
        # Add additional kwargs (may also be lists for whitelisting)
        params.update(kwargs)
        
        # Make request to bulk endpoint
        bulk_endpoint = f"{endpoint.rstrip('/')}/bulk"
        data = self._make_request(bulk_endpoint, params)
        
        # Process bulk response
        return self._process_bulk_response(data)
    
    def _validate_bulk_timerange(
        self, 
        since: Optional[Union[str, datetime, int]], 
        until: Optional[Union[str, datetime, int]], 
        interval: str
    ):
        """Validate timerange constraints for bulk requests"""
        if not since or not until:
            return  # No validation if dates not provided
        
        start_ts = self._format_timestamp(since)
        end_ts = self._format_timestamp(until)
        duration_days = (end_ts - start_ts) / 86400  # Convert seconds to days
        
        constraints = {
            "10m": 10,
            "1h": 10,
            "24h": 31,
            "1d": 31,
            "1w": 93,
            "1month": 93
        }
        
        max_days = constraints.get(interval)
        if max_days and duration_days > max_days:
            raise ValueError(
                f"Timerange too large for interval '{interval}'. "
                f"Maximum {max_days} days allowed, got {duration_days:.1f} days."
            )
    
    def _process_bulk_response(self, data: Dict) -> pd.DataFrame:
        """Process bulk API response into a DataFrame"""
        if not isinstance(data, dict) or 'data' not in data:
            raise GlassnodeAPIError("Invalid bulk response format")
        
        rows = []
        for time_point in data['data']:
            timestamp = time_point['t']
            bulk_entries = time_point.get('bulk', [])
            
            for entry in bulk_entries:
                row = {'t': timestamp}
                row.update(entry)
                rows.append(row)
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows)
        df['t'] = pd.to_datetime(df['t'], unit='s')
        df.set_index('t', inplace=True)
        
        return df
    
    def _format_timestamp(self, timestamp: Union[str, datetime, int]) -> int:
        """Convert various timestamp formats to unix timestamp"""
        if isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return int(dt.timestamp())
            except ValueError:
                # Try parsing as date only
                dt = datetime.strptime(timestamp, "%Y-%m-%d")
                return int(dt.timestamp())
        elif isinstance(timestamp, datetime):
            return int(timestamp.timestamp())
        elif isinstance(timestamp, int):
            return timestamp
        else:
            raise ValueError(f"Unsupported timestamp format: {type(timestamp)}")


class BaseEndpoints:
    """Base class for endpoint categories"""
    
    def __init__(self, client: GlassnodeClient):
        self.client = client
        self.base_path = ""
    
    def _get_data(self, endpoint: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Helper method to get data with base path"""
        full_endpoint = f"{self.base_path}/{endpoint}" if self.base_path else endpoint
        return self.client.get_data(full_endpoint, **kwargs)
    
    def _get_bulk_data(self, endpoint: str, **kwargs) -> pd.DataFrame:
        """Helper method to get bulk data with base path"""
        full_endpoint = f"{self.base_path}/{endpoint}" if self.base_path else endpoint
        return self.client.get_bulk_data(full_endpoint, **kwargs)


class AddressesEndpoints(BaseEndpoints):
    """Address-related metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/addresses"
    
    def active_count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get active addresses count"""
        return self._get_data("active_count", asset=asset, **kwargs)
    
    def active_count_bulk(self, assets: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
        """Get active addresses count for multiple assets (bulk)"""
        return self._get_bulk_data("active_count", assets=assets, **kwargs)
    
    def count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get total addresses count"""
        return self._get_data("count", asset=asset, **kwargs)
    
    def count_bulk(self, assets: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
        """Get total addresses count for multiple assets (bulk)"""
        return self._get_bulk_data("count", assets=assets, **kwargs)
    
    def new_non_zero_count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get new non-zero addresses count"""
        return self._get_data("new_non_zero_count", asset=asset, **kwargs)
    
    def new_non_zero_count_bulk(self, assets: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
        """Get new non-zero addresses count for multiple assets (bulk)"""
        return self._get_bulk_data("new_non_zero_count", assets=assets, **kwargs)
    
    def sending_count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get sending addresses count"""
        return self._get_data("sending_count", asset=asset, **kwargs)
    
    def sending_count_bulk(self, assets: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
        """Get sending addresses count for multiple assets (bulk)"""
        return self._get_bulk_data("sending_count", assets=assets, **kwargs)
    
    def receiving_count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get receiving addresses count"""
        return self._get_data("receiving_count", asset=asset, **kwargs)
    
    def receiving_count_bulk(self, assets: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
        """Get receiving addresses count for multiple assets (bulk)"""
        return self._get_bulk_data("receiving_count", assets=assets, **kwargs)


class BlockchainEndpoints(BaseEndpoints):
    """Blockchain metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/blockchain"
    
    def block_height(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get current block height"""
        return self._get_data("block_height", asset=asset, **kwargs)
    
    def block_interval_mean(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get mean block interval"""
        return self._get_data("block_interval_mean", asset=asset, **kwargs)
    
    def block_size_mean(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get mean block size"""
        return self._get_data("block_size_mean", asset=asset, **kwargs)
    
    def utxo_count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get UTXO count"""
        return self._get_data("utxo_count", asset=asset, **kwargs)


class MarketEndpoints(BaseEndpoints):
    """Market data endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/market"
    
    def price(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get asset price in USD"""
        return self._get_data("price_usd_close", asset=asset, **kwargs)
    
    def price_bulk(self, assets: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
        """Get asset price in USD for multiple assets (bulk)"""
        return self._get_bulk_data("price_usd_close", assets=assets, **kwargs)
    
    def market_cap(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get market capitalization"""
        return self._get_data("marketcap_usd", asset=asset, **kwargs)
    
    def market_cap_bulk(self, assets: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
        """Get market capitalization for multiple assets (bulk)"""
        return self._get_bulk_data("marketcap_usd", assets=assets, **kwargs)
    
    def mvrv(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get Market Value to Realized Value ratio"""
        return self._get_data("mvrv", asset=asset, **kwargs)
    
    def mvrv_bulk(self, assets: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
        """Get Market Value to Realized Value ratio for multiple assets (bulk)"""
        return self._get_bulk_data("mvrv", assets=assets, **kwargs)
    
    def mvrv_z_score(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get MVRV Z-Score"""
        return self._get_data("mvrv_z_score", asset=asset, **kwargs)
    
    def mvrv_z_score_bulk(self, assets: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
        """Get MVRV Z-Score for multiple assets (bulk)"""
        return self._get_bulk_data("mvrv_z_score", assets=assets, **kwargs)
    
    def realized_cap(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get realized capitalization"""
        return self._get_data("realizedcap_usd", asset=asset, **kwargs)
    
    def realized_cap_bulk(self, assets: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
        """Get realized capitalization for multiple assets (bulk)"""
        return self._get_bulk_data("realizedcap_usd", assets=assets, **kwargs)
    
    def realized_price(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get realized price"""
        return self._get_data("price_realized_usd", asset=asset, **kwargs)
    
    def realized_price_bulk(self, assets: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
        """Get realized price for multiple assets (bulk)"""
        return self._get_bulk_data("price_realized_usd", assets=assets, **kwargs)
    
    def btc_dominance(self, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get Bitcoin dominance"""
        return self._get_data("btc_dominance", asset="BTC", **kwargs)
    
    def price_drawdown(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get price drawdown from ATH"""
        return self._get_data("price_drawdown_relative", asset=asset, **kwargs)
    
    def price_drawdown_bulk(self, assets: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
        """Get price drawdown from ATH for multiple assets (bulk)"""
        return self._get_bulk_data("price_drawdown_relative", assets=assets, **kwargs)


class IndicatorsEndpoints(BaseEndpoints):
    """Technical indicators endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/indicators"
    
    def sopr(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get Spent Output Profit Ratio"""
        return self._get_data("sopr", asset=asset, **kwargs)
    
    def asopr(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get Adjusted SOPR"""
        return self._get_data("sopr_adjusted", asset=asset, **kwargs)
    
    def nupl(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get Net Unrealized Profit/Loss"""
        return self._get_data("net_unrealized_profit_loss", asset=asset, **kwargs)
    
    def puell_multiple(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get Puell Multiple"""
        return self._get_data("puell_multiple", asset=asset, **kwargs)
    
    def nvt(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get Network Value to Transactions ratio"""
        return self._get_data("nvt", asset=asset, **kwargs)
    
    def nvts(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get NVT Signal"""
        return self._get_data("nvts", asset=asset, **kwargs)


class SupplyEndpoints(BaseEndpoints):
    """Supply metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/supply"
    
    def current(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get current supply"""
        return self._get_data("current", asset=asset, **kwargs)
    
    def active_24h(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get 24h active supply"""
        return self._get_data("active_24h", asset=asset, **kwargs)
    
    def active_1d_1w(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get supply active between 1 day and 1 week"""
        return self._get_data("active_1d_1w", asset=asset, **kwargs)
    
    def illiquid(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get illiquid supply"""
        return self._get_data("illiquid", asset=asset, **kwargs)
    
    def liquid(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get liquid supply"""
        return self._get_data("liquid", asset=asset, **kwargs)


class TransactionsEndpoints(BaseEndpoints):
    """Transaction metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/transactions"
    
    def count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get transaction count"""
        return self._get_data("count", asset=asset, **kwargs)
    
    def transfers_volume_usd(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get transfer volume in USD"""
        return self._get_data("transfers_volume_usd", asset=asset, **kwargs)
    
    def transfers_volume_mean_usd(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get mean transfer volume in USD"""
        return self._get_data("transfers_volume_mean_usd", asset=asset, **kwargs)
    
    def size_mean(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get mean transaction size"""
        return self._get_data("size_mean", asset=asset, **kwargs)


class FeesEndpoints(BaseEndpoints):
    """Fee metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/fees"
    
    def volume_usd(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get fee volume in USD"""
        return self._get_data("volume_usd", asset=asset, **kwargs)
    
    def volume_mean_usd(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get mean fee in USD"""
        return self._get_data("volume_mean_usd", asset=asset, **kwargs)
    
    def gas_price_mean(self, asset: str = "ETH", **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get mean gas price (ETH only)"""
        return self._get_data("gas_price_mean", asset=asset, **kwargs)


class MiningEndpoints(BaseEndpoints):
    """Mining metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/mining"
    
    def difficulty(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get mining difficulty"""
        return self._get_data("difficulty_latest", asset=asset, **kwargs)
    
    def hash_rate(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get hash rate"""
        return self._get_data("hash_rate_mean", asset=asset, **kwargs)
    
    def revenue_usd(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get mining revenue in USD"""
        return self._get_data("revenue_usd", asset=asset, **kwargs)


class DistributionEndpoints(BaseEndpoints):
    """Distribution metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/distribution"
    
    def balance_exchanges(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get exchange balances"""
        return self._get_data("balance_exchanges", asset=asset, **kwargs)
    
    def balance_1pct_holders(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get balance held by top 1% holders"""
        return self._get_data("balance_1pct_holders", asset=asset, **kwargs)


class EntitiesEndpoints(BaseEndpoints):
    """Entity metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/entities"
    
    def active_count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get active entities count"""
        return self._get_data("active_count", asset=asset, **kwargs)


class DeFiEndpoints(BaseEndpoints):
    """DeFi metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/defi"
    
    def total_value_locked_usd(self, asset: str = "ETH", **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get total value locked in DeFi"""
        return self._get_data("total_value_locked_usd", asset=asset, **kwargs)


class DerivativesEndpoints(BaseEndpoints):
    """Derivatives metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/derivatives"
    
    def futures_open_interest_sum(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get futures open interest"""
        return self._get_data("futures_open_interest_sum", asset=asset, **kwargs)


class InstitutionsEndpoints(BaseEndpoints):
    """Institutional metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/institutions"
    
    def purpose_etf_holdings_sum(self, asset: str = "BTC", **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get Purpose ETF holdings"""
        return self._get_data("purpose_etf_holdings_sum", asset=asset, **kwargs)


class MempoolEndpoints(BaseEndpoints):
    """Mempool metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/mempool"
    
    def count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get mempool transaction count"""
        return self._get_data("count", asset=asset, **kwargs)
    
    def size_bytes(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get mempool size in bytes"""
        return self._get_data("size_bytes", asset=asset, **kwargs)


class ProtocolsEndpoints(BaseEndpoints):
    """Protocol metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/protocols"


class SignalsEndpoints(BaseEndpoints):
    """Signal endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/signals"


class MetadataEndpoints(BaseEndpoints):
    """Metadata endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metadata"
    
    def assets(self, filter_expr: Optional[str] = None) -> Dict:
        """Get available assets metadata"""
        params = {"filter": filter_expr} if filter_expr else {}
        return self.client._make_request("metadata/assets", params)
    
    def metrics(self) -> List[str]:
        """Get available metric paths"""
        return self.client._make_request("metadata/metrics")
    
    def metric(self, path: str, **kwargs) -> Dict:
        """Get metadata for a specific metric"""
        params = {"path": path}
        params.update(kwargs)
        return self.client._make_request("metadata/metric", params) 