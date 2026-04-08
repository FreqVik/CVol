import pandas as pd
import yfinance as yf
import logging
import sys
import time
import threading
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

class ChartService:
    def __init__(self):
        self._frames = {}  # {(symbol, timeframe): DataFrame}
        self._initialized = False
        self._lock = threading.Lock()  # Protect concurrent access to _frames

    def _frame_key(self, symbol: str, timeframe: str) -> tuple[str, str]:
        return symbol, timeframe

    def _convert_symbol(self, symbol: str) -> str:
        """Convert CCXT-style symbols (BTC/USDT) to yfinance style (BTC-USD)"""
        if '/' in symbol:
            base, quote = symbol.split('/')
            quote_map = {'USDT': 'USD', 'BUSD': 'USD', 'USDC': 'USD'}
            quote = quote_map.get(quote, quote)
            return f"{base}-{quote}"
        return symbol

    def _sanitize_ohlcv(self, df) -> pd.DataFrame:
        """Normalize yfinance output to standard format"""
        if df.empty:
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        df = df.copy()
        
        # Handle MultiIndex case (when downloading with interval like 1h)
        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index()
        else:
            df = df.reset_index()
        
        # Flatten MultiIndex in columns - get the first level which has OHLCV names
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        # Convert all column names to lowercase for consistency
        df.columns = [str(col).lower().strip() for col in df.columns]
        
        # Rename to standard names
        rename_map = {
            'date': 'timestamp',
            'datetime': 'timestamp',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        
        # Convert timestamp to datetime with UTC
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        else:
            raise ValueError(f"Cannot find timestamp column. Available: {df.columns.tolist()}")
        
        # Select required columns only
        required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df = df[required_cols].sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last').reset_index(drop=True)
        
        return df

    def _fetch_raw_data(self, symbol='BTC/USDT', timeframe='1h', days=30, retry_count=5):
        """Fetch raw OHLCV data from yfinance with retry logic"""
        ticker = self._convert_symbol(symbol)
        interval_map = {'1h': '1h', '4h': '1h', '1d': '1d', '1w': '1wk'}
        interval = interval_map.get(timeframe, '1h')
        
        logger.debug(f"Fetching {days}d of {ticker} data at {interval} interval")
        
        for attempt in range(1, retry_count + 1):
            try:
                df = yf.download(ticker, period=f"{days}d", interval=interval, progress=False)
                if df.empty:
                    raise ValueError(f"No data returned for {ticker}")
                logger.debug(f"✓ Downloaded {len(df)} candles for {ticker}")
                return self._sanitize_ohlcv(df)
            except Exception as e:
                if attempt < retry_count:
                    wait_time = 2 ** (attempt - 1)  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                    logger.warning(f"✗ Attempt {attempt}/{retry_count} failed for {ticker}: {str(e)}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"✗ All {retry_count} attempts failed for {ticker}: {str(e)}")
                    logger.critical(f"✗ FATAL: Cannot fetch {ticker} data after {retry_count} retries. Shutting down server.")
                    sys.exit(1)

    def _calculate_metrics(self, df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
        """Calculate returns and realized volatility"""
        if df.empty:
            logger.warning("Cannot calculate metrics on empty DataFrame")
            return df
        
        logger.debug(f"Calculating returns and realized_vol (window={window}) for {len(df)} candles")
        df = df.copy()
        df['returns'] = df['close'].pct_change()
        df['realized_vol'] = df['returns'].rolling(window).std()
        logger.debug(f"✓ Metrics calculated. Non-null realized_vol rows: {df['realized_vol'].notna().sum()}")
        return df

    def _trim_to_window(self, df: pd.DataFrame, days: int = 30) -> pd.DataFrame:
        """Keep DataFrame within the last N days"""
        if df.empty:
            return df
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        return df[df['timestamp'] >= cutoff].reset_index(drop=True)

    async def initialize(self, symbol='BTC/USDT', timeframe='1h', days=30, window=20):
        """Load initial 30-day data on app startup with retries"""
        key = self._frame_key(symbol, timeframe)
        logger.info(f"Initializing ChartService for {symbol} {timeframe} (fetching {days} days)")
        try:
            df = self._fetch_raw_data(symbol=symbol, timeframe=timeframe, days=days, retry_count=5)
            logger.debug(f"Fetched {len(df)} raw candles")
            
            df = self._calculate_metrics(df, window=window)
            logger.debug(f"Calculated metrics with window={window}")
            
            self._frames[key] = df
            self._initialized = True
            
            logger.info(f"✓ ChartService initialized: {len(df)} candles for {symbol} {timeframe}")
            return {"status": "initialized", "rows": len(df), "symbol": symbol}
        except SystemExit:
            raise  # Re-raise SystemExit to ensure server shutdown
        except Exception as e:
            logger.error(f"✗ Initialization failed for {symbol}: {str(e)}", exc_info=True)
            logger.critical(f"✗ FATAL: Chart service initialization failed. Shutting down server.")
            sys.exit(1)

    def get_data(self, symbol='BTC/USDT', timeframe='1h', limit=None):
        """Get DataFrame for symbol/timeframe"""
        key = self._frame_key(symbol, timeframe)
        with self._lock:
            if key not in self._frames:
                logger.warning(f"Data requested for {symbol}/{timeframe} but not initialized")
                raise ValueError(f"No data for {symbol}/{timeframe}. Please initialize the service first.")
            
            df = self._frames[key].copy()  # Return copy to prevent external modification
        
        if limit and limit > 0:
            df = df.tail(limit)
            logger.debug(f"Retrieved last {limit} rows for {symbol}/{timeframe}")
        else:
            logger.debug(f"Retrieved all {len(df)} rows for {symbol}/{timeframe}")
        return df

    def append_new_data(self, symbol='BTC/USDT', timeframe='1h', window=20):
        """Fetch new data, append to existing, trim to 30-day window (with retries)"""
        key = self._frame_key(symbol, timeframe)
        
        with self._lock:
            if key not in self._frames:
                logger.error(f"No data for {symbol}/{timeframe}. Service not initialized")
                raise ValueError(f"No data for {symbol}/{timeframe}. Please initialize the service first.")
            
            current_df = self._frames[key].copy()
        
        current_len = len(current_df)
        logger.debug(f"Current DataFrame size: {current_len} rows, date range: {current_df['timestamp'].min()} to {current_df['timestamp'].max()}")
        
        # Fetch last 7 days to ensure we capture new candles (with retries)
        try:
            new_df = self._fetch_raw_data(symbol=symbol, timeframe=timeframe, days=7, retry_count=5)
        except SystemExit:
            logger.critical(f"✗ FATAL: Cannot fetch new data after retries. Shutting down server.")
            raise
        
        new_df = self._calculate_metrics(new_df, window=window)
        logger.debug(f"Fetched {len(new_df)} new candles")
        
        # Merge and deduplicate
        merged = pd.concat([current_df, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=['timestamp'], keep='last').sort_values('timestamp').reset_index(drop=True)
        logger.debug(f"After dedup: {len(merged)} rows (removed {len(current_df) + len(new_df) - len(merged)} duplicates)")
        
        # Trim to 30 days
        original_len = len(merged)
        merged = self._trim_to_window(merged, days=30)
        trimmed_count = original_len - len(merged)
        if trimmed_count > 0:
            logger.debug(f"Trimmed {trimmed_count} rows to maintain 30-day window")
        
        appended_count = len(merged) - current_len
        
        # Update shared state with lock
        with self._lock:
            self._frames[key] = merged
        
        logger.info(f"✓ Append complete: {appended_count} new rows. Total: {len(merged)} rows. Date range: {merged['timestamp'].min()} to {merged['timestamp'].max()}")
        return merged, max(0, appended_count)