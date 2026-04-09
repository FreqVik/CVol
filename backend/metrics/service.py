import math
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .model import (
    DEFAULT_DB_PATH,
    MetricSnapshot,
    PredictionMetric,
    get_metrics_session_factory,
    init_metrics_db,
)

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[2]
PREDICTIONS_DB_PATH = BASE_DIR / 'backend' / 'data' / 'predictions.db'
METRICS_DB_PATH = DEFAULT_DB_PATH

# Singleton instance
_metrics_service_instance = None


class MetricsService:
    """Evaluate prediction accuracy by comparing predicted vs realized volatility"""
    
    def __init__(self, metrics_db_path=None, chart_service=None):
        self.metrics_db_path = Path(metrics_db_path) if metrics_db_path else METRICS_DB_PATH
        self.chart_service = chart_service
        
        logger.debug(f"Initializing MetricsService: metrics_db={self.metrics_db_path}")
        init_metrics_db(self.metrics_db_path)
        self.SessionLocal = get_metrics_session_factory(self.metrics_db_path)
        logger.info("✓ MetricsService initialized")

    def _load_predictions(self, limit=200):
        """Load recent predictions from predictions.db"""
        from sqlalchemy import create_engine
        
        predictions_db = BASE_DIR / 'backend' / 'data' / 'predictions.db'
        if not predictions_db.exists():
            logger.warning("Predictions database not found")
            return pd.DataFrame(columns=['id', 'prediction_time', 'predicted_volatility'])

        try:
            engine = create_engine(f"sqlite:///{predictions_db}", future=True)
            query = text(
                """
                SELECT id, prediction_time, predicted_volatility
                FROM predictions
                ORDER BY id DESC
                LIMIT :limit
                """
            )
            with engine.connect() as conn:
                rows = conn.execute(query, {'limit': limit}).mappings().all()

            df = pd.DataFrame(rows)
            if df.empty:
                logger.debug("No predictions found in database")
                return df

            df['prediction_time'] = pd.to_datetime(df['prediction_time'], utc=True, errors='coerce')
            df = df.dropna(subset=['prediction_time']).sort_values('prediction_time').reset_index(drop=True)
            logger.debug(f"Loaded {len(df)} predictions")
            return df
        except Exception as e:
            logger.error(f"Failed to load predictions: {str(e)}", exc_info=True)
            raise

    def compute_and_store_metrics(self, symbol='BTC/USDT', timeframe='1h', window=20, prediction_limit=200):
        """Compute accuracy metrics comparing predictions vs realized volatility"""
        logger.info(f"Computing metrics: {symbol} {timeframe} (window={window}, prediction_limit={prediction_limit})")
        
        try:
            # Load predictions
            predictions = self._load_predictions(limit=prediction_limit)
            if predictions.empty:
                logger.warning("No predictions available to evaluate")
                return {
                    'status': 'skipped',
                    'message': 'No predictions available to evaluate.',
                    'prediction_count': 0,
                }

            logger.debug(f"Loaded {len(predictions)} predictions")

            # Get realized volatility from chart service
            if self.chart_service is None:
                logger.error("ChartService not available")
                raise ValueError("ChartService required for metrics computation")
            
            try:
                df_chart = self.chart_service.get_data(symbol=symbol, timeframe=timeframe)
            except Exception as e:
                logger.error(f"Failed to get chart data: {str(e)}")
                return {
                    'status': 'failed',
                    'message': f'Failed to get chart data: {str(e)}',
                    'prediction_count': 0,
                }

            if df_chart.empty:
                logger.warning("No chart data available for metrics")
                return {
                    'status': 'skipped',
                    'message': 'No realized volatility data available.',
                    'prediction_count': 0,
                }

            # Prepare realized vol data
            realized_vol = df_chart[['timestamp', 'realized_vol']].rename(
                columns={'realized_vol': 'realized_volatility'}
            ).dropna(subset=['realized_volatility'])
            
            logger.debug(f"Chart data has {len(realized_vol)} realized vol points")

            # Normalize datetime precision for merge (convert both to microseconds)
            predictions['prediction_time'] = predictions['prediction_time'].astype('datetime64[us, UTC]')
            realized_vol['timestamp'] = realized_vol['timestamp'].astype('datetime64[us, UTC]')

            # Merge predictions with realized volatility
            merged = pd.merge_asof(
                predictions.sort_values('prediction_time'),
                realized_vol.sort_values('timestamp'),
                left_on='prediction_time',
                right_on='timestamp',
                direction='backward',
            )
            merged = merged.dropna(subset=['realized_volatility']).reset_index(drop=True)
            
            if merged.empty:
                logger.warning("No overlapping predictions and realized volatility")
                return {
                    'status': 'skipped',
                    'message': 'No overlapping predictions and realized volatility points.',
                    'prediction_count': 0,
                }

            logger.debug(f"Merged {len(merged)} prediction-volatility pairs")

            # Calculate error metrics
            merged['abs_error'] = (merged['predicted_volatility'] - merged['realized_volatility']).abs()
            merged['squared_error'] = (merged['predicted_volatility'] - merged['realized_volatility']) ** 2
            merged['ape'] = merged.apply(
                lambda row: abs(row['predicted_volatility'] - row['realized_volatility']) / abs(row['realized_volatility'])
                if row['realized_volatility'] != 0
                else None,
                axis=1,
            )

            # Directional accuracy (did predicted and realized move in same direction?)
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

            # Aggregate metrics
            mae = float(merged['abs_error'].mean())
            rmse = float(math.sqrt(merged['squared_error'].mean()))
            mape_series = merged['ape'].dropna()
            mape = float(mape_series.mean()) if not mape_series.empty else None

            logger.debug(f"Computed metrics: MAE={mae:.6f}, RMSE={rmse:.6f}, MAPE={mape}, DirectionalAccuracy={directional_accuracy}")

            # Persist metrics
            computed_at = datetime.now(timezone.utc)
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

            logger.info(f"✓ Metrics computed and stored: id={snapshot_id}, predictions={len(merged)}, MAE={mae:.6f}")
            
            # Note: Model retraining now runs as a separate background job (see scheduler)
            # This prevents blocking the metrics computation
            
            return {
                'status': 'completed',
                'id': snapshot_id,
                'computed_at': computed_at.isoformat(),
                'symbol': symbol,
                'timeframe': timeframe,
                'window': window,
                'prediction_count': int(len(merged)),
                'mae': mae,
                'rmse': rmse,
                'mape': mape,
                'directional_accuracy': directional_accuracy,
            }
        except Exception as e:
            logger.error(f"Failed to compute metrics: {str(e)}", exc_info=True)
            raise

    def _store_prediction_metrics(self, merged_df, symbol, timeframe, window, created_at):
        """Store individual prediction-volatility pairs"""
        payloads = []
        for row in merged_df.itertuples(index=False):
            ape_value = None if pd.isna(row.ape) else float(row.ape)
            payloads.append(
                {
                    'prediction_id': int(row.id),
                    'prediction_time': row.prediction_time,
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
            logger.debug("No metrics to store")
            return

        try:
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
            
            logger.debug(f"Stored {len(payloads)} prediction metrics")
        except Exception as e:
            logger.error(f"Failed to store prediction metrics: {str(e)}", exc_info=True)
            raise

    def _store_snapshot(self, computed_at, symbol, timeframe, window, prediction_count, mae, rmse, mape, directional_accuracy):
        """Store aggregated metrics snapshot"""
        try:
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
                logger.debug(f"Stored metric snapshot: id={snapshot.id}")
                return snapshot.id
        except Exception as e:
            logger.error(f"Failed to store snapshot: {str(e)}", exc_info=True)
            raise

    def _trigger_model_retrain(self, symbol='BTC/USDT', timeframe='1h'):
        """Trigger GARCH model retraining with latest returns data"""
        try:
            logger.info(f"Triggering model retrain with latest {symbol} {timeframe} data...")
            
            # Get latest chart data
            df_chart = self.chart_service.get_data(symbol=symbol, timeframe=timeframe)
            
            if df_chart.empty or 'returns' not in df_chart.columns:
                logger.warning("No returns data available for model retraining")
                return False
            
            # Extract returns (NOT realized_vol) - GARCH needs raw returns, not pre-computed volatility
            returns_series = df_chart['returns'].dropna()
            
            if len(returns_series) < 20:
                logger.warning(f"Insufficient data for retraining: {len(returns_series)} points")
                return False
            
            # Import predictor and retrain with returns data
            from predict.service import get_predictor
            predictor = get_predictor()
            
            success = predictor.retrain_model(returns_series)
            if success:
                logger.info(f"✓ Model retrained successfully with {len(returns_series)} returns data points")
            else:
                logger.warning("Model retraining failed or skipped")
            
            return success
        except Exception as e:
            logger.error(f"Failed to trigger model retrain: {str(e)}", exc_info=True)
            return False

    def get_latest_metrics(self, symbol='BTC/USDT', timeframe='1h', window=20):
        """Get most recent metric snapshot"""
        try:
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
                logger.debug(f"No metrics found for {symbol}/{timeframe}")
                return None

            logger.debug(f"Retrieved latest metrics: id={row.id}")
            return row.to_dict()
        except Exception as e:
            logger.error(f"Failed to fetch latest metrics: {str(e)}", exc_info=True)
            raise

    def get_metrics_history(self, symbol='BTC/USDT', timeframe='1h', window=20, limit=50):
        """Get historical metric snapshots"""
        if not 1 <= limit <= 1000:
            logger.warning(f"Invalid limit {limit}, clamping to valid range")
            limit = max(1, min(limit, 1000))

        try:
            with self.SessionLocal() as session:
                rows = session.execute(
                    select(MetricSnapshot)
                    .where(MetricSnapshot.symbol == symbol)
                    .where(MetricSnapshot.timeframe == timeframe)
                    .where(MetricSnapshot.window == window)
                    .order_by(MetricSnapshot.id.desc())
                    .limit(limit)
                ).scalars().all()

            logger.debug(f"Retrieved {len(rows)} metric snapshots")
            return [row.to_dict() for row in rows]
        except Exception as e:
            logger.error(f"Failed to fetch metrics history: {str(e)}", exc_info=True)
            raise

    def get_prediction_metrics(self, symbol='BTC/USDT', timeframe='1h', window=20, limit=100):
        """Get individual prediction-volatility metrics"""
        if not 1 <= limit <= 10000:
            logger.warning(f"Invalid limit {limit}, clamping to valid range")
            limit = max(1, min(limit, 10000))

        try:
            with self.SessionLocal() as session:
                rows = session.execute(
                    select(PredictionMetric)
                    .where(PredictionMetric.symbol == symbol)
                    .where(PredictionMetric.timeframe == timeframe)
                    .where(PredictionMetric.window == window)
                    .order_by(PredictionMetric.prediction_id.desc())
                    .limit(limit)
                ).scalars().all()

            logger.debug(f"Retrieved {len(rows)} prediction metrics")
            return [row.to_dict() for row in rows]
        except Exception as e:
            logger.error(f"Failed to fetch prediction metrics: {str(e)}", exc_info=True)
            raise


def get_metrics_service(chart_service=None):
    """Get or create singleton MetricsService instance"""
    global _metrics_service_instance
    if _metrics_service_instance is None:
        logger.info("Creating new MetricsService singleton instance")
        _metrics_service_instance = MetricsService(chart_service=chart_service)
    return _metrics_service_instance
