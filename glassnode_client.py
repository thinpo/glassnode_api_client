"""
Glassnode API Python Client

A comprehensive Python client for interacting with the Glassnode API.
Provides access to on-chain and market data for cryptocurrencies.

Documentation: https://docs.glassnode.com/basic-api/endpoints
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
        
        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
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


class AddressesEndpoints(BaseEndpoints):
    """Address-related metrics endpoints"""
    
    def __init__(self, client: GlassnodeClient):
        super().__init__(client)
        self.base_path = "metrics/addresses"
    
    def active_count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get active addresses count"""
        return self._get_data("active_count", asset=asset, **kwargs)
    
    def count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get total addresses count"""
        return self._get_data("count", asset=asset, **kwargs)
    
    def new_non_zero_count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get new non-zero addresses count"""
        return self._get_data("new_non_zero_count", asset=asset, **kwargs)
    
    def sending_count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get sending addresses count"""
        return self._get_data("sending_count", asset=asset, **kwargs)
    
    def receiving_count(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get receiving addresses count"""
        return self._get_data("receiving_count", asset=asset, **kwargs)


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
    
    def market_cap(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get market capitalization"""
        return self._get_data("marketcap_usd", asset=asset, **kwargs)
    
    def mvrv(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get Market Value to Realized Value ratio"""
        return self._get_data("mvrv", asset=asset, **kwargs)
    
    def mvrv_z_score(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get MVRV Z-Score"""
        return self._get_data("mvrv_z_score", asset=asset, **kwargs)
    
    def realized_cap(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get realized capitalization"""
        return self._get_data("realizedcap_usd", asset=asset, **kwargs)
    
    def realized_price(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get realized price"""
        return self._get_data("price_realized_usd", asset=asset, **kwargs)
    
    def btc_dominance(self, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get Bitcoin dominance"""
        return self._get_data("btc_dominance", asset="BTC", **kwargs)
    
    def price_drawdown(self, asset: str, **kwargs) -> Union[pd.DataFrame, List[Dict]]:
        """Get price drawdown from ATH"""
        return self._get_data("price_drawdown_relative", asset=asset, **kwargs)


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