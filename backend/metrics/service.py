import math
from datetime import datetime, timezone
from pathlib import Path

import ccxt
import pandas as pd
from sqlalchemy import create_engine, select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .model import (
    DEFAULT_DB_PATH,
    MetricSnapshot,
    PredictionMetric,
    get_metrics_session_factory,
    init_metrics_db,
)


BASE_DIR = Path(__file__).resolve().parents[2]
PREDICTIONS_DB_PATH = BASE_DIR / 'backend' / 'data' / 'predictions.db'
METRICS_DB_PATH = DEFAULT_DB_PATH


class MetricsService:
    def __init__(self, predictions_db_path=None, metrics_db_path=None):
        self.predictions_db_path = Path(predictions_db_path) if predictions_db_path else PREDICTIONS_DB_PATH
        self.metrics_db_path = Path(metrics_db_path) if metrics_db_path else METRICS_DB_PATH
        self.exchange = ccxt.binance()

        init_metrics_db(self.metrics_db_path)
        self.SessionLocal = get_metrics_session_factory(self.metrics_db_path)
        self.predictions_engine = create_engine(f"sqlite:///{self.predictions_db_path}", future=True)

    def _load_predictions(self, limit=200):
        if not self.predictions_db_path.exists():
            return pd.DataFrame(columns=['id', 'prediction_time', 'predicted_volatility'])

        query = text(
            """
            SELECT id, prediction_time, predicted_volatility
            FROM predictions
            ORDER BY id DESC
            LIMIT :limit
            """
        )
        with self.predictions_engine.connect() as conn:
            rows = conn.execute(query, {'limit': limit}).mappings().all()

        df = pd.DataFrame(rows)
        if df.empty:
            return df

        df['prediction_time'] = pd.to_datetime(df['prediction_time'], utc=True, errors='coerce')
        df = df.dropna(subset=['prediction_time']).sort_values('prediction_time').reset_index(drop=True)
        return df

    def _fetch_realized_vol(self, symbol='BTC/USDT', timeframe='4h', window=20, limit=1000):
        ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        if df.empty:
            return pd.DataFrame(columns=['timestamp', 'realized_volatility'])

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df = df.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last').reset_index(drop=True)
        df['returns'] = df['close'].pct_change()
        df['realized_volatility'] = df['returns'].rolling(window).std()
        return df[['timestamp', 'realized_volatility']].dropna().reset_index(drop=True)

    def compute_and_store_metrics(self, symbol='BTC/USDT', timeframe='4h', window=20, prediction_limit=200):
        predictions = self._load_predictions(limit=prediction_limit)
        if predictions.empty:
            return {
                'message': 'No predictions available to evaluate.',
                'prediction_count': 0,
            }

        realized_vol = self._fetch_realized_vol(symbol=symbol, timeframe=timeframe, window=window)
        if realized_vol.empty:
            return {
                'message': 'No realized volatility data available.',
                'prediction_count': 0,
            }

        merged = pd.merge_asof(
            predictions.sort_values('prediction_time'),
            realized_vol.sort_values('timestamp'),
            left_on='prediction_time',
            right_on='timestamp',
            direction='backward',
        )
        merged = merged.dropna(subset=['realized_volatility']).reset_index(drop=True)
        if merged.empty:
            return {
                'message': 'No overlapping predictions and realized volatility points.',
                'prediction_count': 0,
            }

        merged['abs_error'] = (merged['predicted_volatility'] - merged['realized_volatility']).abs()
        merged['squared_error'] = (merged['predicted_volatility'] - merged['realized_volatility']) ** 2
        merged['ape'] = merged.apply(
            lambda row: abs(row['predicted_volatility'] - row['realized_volatility']) / abs(row['realized_volatility'])
            if row['realized_volatility'] != 0
            else None,
            axis=1,
        )

        pred_delta = merged['predicted_volatility'].diff()
        real_delta = merged['realized_volatility'].diff()
        directional_mask = pred_delta.notna() & real_delta.notna()
        if directional_mask.any():
            directional_accuracy = (
                (pred_delta[directional_mask] > 0) == (real_delta[directional_mask] > 0)
            ).mean()
            directional_accuracy = float(directional_accuracy)
        else:
            directional_accuracy = None

        mae = float(merged['abs_error'].mean())
        rmse = float(math.sqrt(merged['squared_error'].mean()))
        mape_series = merged['ape'].dropna()
        mape = float(mape_series.mean()) if not mape_series.empty else None

        computed_at = datetime.now(timezone.utc).isoformat()
        self._store_prediction_metrics(
            merged,
            symbol=symbol,
            timeframe=timeframe,
            window=window,
            created_at=computed_at,
        )
        snapshot_id = self._store_snapshot(
            computed_at=computed_at,
            symbol=symbol,
            timeframe=timeframe,
            window=window,
            prediction_count=int(len(merged)),
            mae=mae,
            rmse=rmse,
            mape=mape,
            directional_accuracy=directional_accuracy,
        )

        return {
            'id': snapshot_id,
            'computed_at': computed_at,
            'symbol': symbol,
            'timeframe': timeframe,
            'window': window,
            'prediction_count': int(len(merged)),
            'mae': mae,
            'rmse': rmse,
            'mape': mape,
            'directional_accuracy': directional_accuracy,
        }

    def _store_prediction_metrics(self, merged_df, symbol, timeframe, window, created_at):
        payloads = []
        for row in merged_df.itertuples(index=False):
            ape_value = None if pd.isna(row.ape) else float(row.ape)
            payloads.append(
                {
                    'prediction_id': int(row.id),
                    'prediction_time': row.prediction_time.isoformat(),
                    'predicted_volatility': float(row.predicted_volatility),
                    'realized_volatility': float(row.realized_volatility),
                    'abs_error': float(row.abs_error),
                    'squared_error': float(row.squared_error),
                    'ape': ape_value,
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'window': int(window),
                    'created_at': created_at,
                }
            )

        if not payloads:
            return

        insert_stmt = sqlite_insert(PredictionMetric).values(payloads)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['prediction_id', 'symbol', 'timeframe', 'window'],
            set_={
                'prediction_time': insert_stmt.excluded.prediction_time,
                'predicted_volatility': insert_stmt.excluded.predicted_volatility,
                'realized_volatility': insert_stmt.excluded.realized_volatility,
                'abs_error': insert_stmt.excluded.abs_error,
                'squared_error': insert_stmt.excluded.squared_error,
                'ape': insert_stmt.excluded.ape,
                'created_at': insert_stmt.excluded.created_at,
            },
        )

        with self.SessionLocal() as session:
            session.execute(upsert_stmt)
            session.commit()

    def _store_snapshot(self, computed_at, symbol, timeframe, window, prediction_count, mae, rmse, mape, directional_accuracy):
        with self.SessionLocal() as session:
            snapshot = MetricSnapshot(
                computed_at=computed_at,
                symbol=symbol,
                timeframe=timeframe,
                window=int(window),
                prediction_count=int(prediction_count),
                mae=float(mae),
                rmse=float(rmse),
                mape=mape,
                directional_accuracy=directional_accuracy,
            )
            session.add(snapshot)
            session.commit()
            session.refresh(snapshot)
            return snapshot.id

    def get_latest_metrics(self, symbol='BTC/USDT', timeframe='4h', window=20):
        with self.SessionLocal() as session:
            row = session.execute(
                select(MetricSnapshot)
                .where(MetricSnapshot.symbol == symbol)
                .where(MetricSnapshot.timeframe == timeframe)
                .where(MetricSnapshot.window == window)
                .order_by(MetricSnapshot.id.desc())
                .limit(1)
            ).scalar_one_or_none()

        if row is None:
            return None

        return {
            'id': row.id,
            'computed_at': row.computed_at,
            'symbol': row.symbol,
            'timeframe': row.timeframe,
            'window': row.window,
            'prediction_count': row.prediction_count,
            'mae': row.mae,
            'rmse': row.rmse,
            'mape': row.mape,
            'directional_accuracy': row.directional_accuracy,
        }

    def get_metrics_history(self, symbol='BTC/USDT', timeframe='4h', window=20, limit=50):
        with self.SessionLocal() as session:
            rows = session.execute(
                select(MetricSnapshot)
                .where(MetricSnapshot.symbol == symbol)
                .where(MetricSnapshot.timeframe == timeframe)
                .where(MetricSnapshot.window == window)
                .order_by(MetricSnapshot.id.desc())
                .limit(limit)
            ).scalars().all()

        return [
            {
                'id': row.id,
                'computed_at': row.computed_at,
                'symbol': row.symbol,
                'timeframe': row.timeframe,
                'window': row.window,
                'prediction_count': row.prediction_count,
                'mae': row.mae,
                'rmse': row.rmse,
                'mape': row.mape,
                'directional_accuracy': row.directional_accuracy,
            }
            for row in rows
        ]

    def get_prediction_metrics(self, symbol='BTC/USDT', timeframe='4h', window=20, limit=100):
        with self.SessionLocal() as session:
            rows = session.execute(
                select(PredictionMetric)
                .where(PredictionMetric.symbol == symbol)
                .where(PredictionMetric.timeframe == timeframe)
                .where(PredictionMetric.window == window)
                .order_by(PredictionMetric.prediction_id.desc())
                .limit(limit)
            ).scalars().all()

        return [
            {
                'id': row.id,
                'prediction_id': row.prediction_id,
                'prediction_time': row.prediction_time,
                'predicted_volatility': row.predicted_volatility,
                'realized_volatility': row.realized_volatility,
                'abs_error': row.abs_error,
                'squared_error': row.squared_error,
                'ape': row.ape,
                'created_at': row.created_at,
            }
            for row in rows
        ]
