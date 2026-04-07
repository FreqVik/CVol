import pandas as pd
import ccxt

class ChartService:
    def __init__(self):
        self.exchange = ccxt.binance()
        self._frames = {}

    def _frame_key(self, symbol: str, timeframe: str) -> tuple[str, str]:
        return symbol, timeframe

    def _sanitize_ohlcv(self, ohlcv) -> pd.DataFrame:
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        if df.empty:
            return df
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last').reset_index(drop=True)

    def fetch_data(self, symbol='BTC/USDT', timeframe='4h', limit=100):
        ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        return self._sanitize_ohlcv(ohlcv)

    def get_or_create_dataframe(self, symbol='BTC/USDT', timeframe='1h', limit=100):
        key = self._frame_key(symbol, timeframe)
        if key not in self._frames:
            self._frames[key] = self.fetch_data(symbol=symbol, timeframe=timeframe, limit=limit)
        return self._frames[key]

    def append_new_data(self, symbol='BTC/USDT', timeframe='1h', fetch_limit=10):
        key = self._frame_key(symbol, timeframe)
        current_df = self.get_or_create_dataframe(symbol=symbol, timeframe=timeframe, limit=fetch_limit)

        if current_df.empty:
            refreshed_df = self.fetch_data(symbol=symbol, timeframe=timeframe, limit=fetch_limit)
            self._frames[key] = refreshed_df
            return refreshed_df, len(refreshed_df)

        last_timestamp_ms = int(current_df['timestamp'].astype('int64').iloc[-1] // 1_000_000)
        new_ohlcv = self.exchange.fetch_ohlcv(
            symbol,
            timeframe=timeframe,
            since=last_timestamp_ms + 1,
            limit=fetch_limit,
        )

        if not new_ohlcv:
            return current_df, 0

        new_df = self._sanitize_ohlcv(new_ohlcv)
        merged_df = (
            pd.concat([current_df, new_df], ignore_index=True)
            .sort_values('timestamp')
            .drop_duplicates(subset=['timestamp'], keep='last')
            .reset_index(drop=True)
        )
        appended_count = max(0, len(merged_df) - len(current_df))
        self._frames[key] = merged_df
        return merged_df, appended_count

    def get_returns(self, df):
        result = df.copy()
        result['returns'] = result['close'].pct_change()
        return result
    
    def compute_realized_vol(self, df, window=20):
        result = df.copy()
        result["returns"] = result["close"].pct_change()
        result["realized_vol"] = (
            result["returns"]
            .rolling(window)
            .std()
        )
        return result