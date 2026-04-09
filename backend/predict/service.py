import joblib
import os
import logging
import pickle
import pandas as pd
import numpy as np
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

from sqlalchemy import select

from .model import (
    DEFAULT_DB_PATH,
    Prediction,
    get_prediction_session_factory,
    init_prediction_db,
)

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_MODEL_DIR = BASE_DIR / 'model'
DEFAULT_DB_PATH = BASE_DIR / 'backend' / 'data' / 'predictions.db'

# Singleton instance
_predictor_instance = None


class Predictor:
    """GARCH volatility forecasting model wrapper with database persistence"""
    
    def __init__(self, model_dir=None, db_path=None):
        self.model_dir = Path(model_dir) if model_dir else DEFAULT_MODEL_DIR
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        logger.debug(f"Initializing Predictor: model_dir={self.model_dir}, db_path={self.db_path}")
        
        init_prediction_db(self.db_path)
        self.SessionLocal = get_prediction_session_factory(self.db_path)
        self.model = self.load_model()
        self._model_lock = threading.Lock()  # Protect concurrent model access during retraining
        
        if self.model is None:
            logger.warning("⚠ Model is None (pickle incompatible or missing). Will be trained on startup with chart data.")
        
        logger.info(f"✓ Predictor initialized successfully")

    def load_model(self):
        """Load GARCH model from pickle file with cross-version compatibility"""
        model_path = self.model_dir / 'garch_btcusdt_1h.pkl'
        logger.debug(f"Loading model from {model_path}")
        
        if not model_path.exists():
            logger.warning(f"Model file not found: {model_path}. Will be trained on startup.")
            return None
        
        try:
            model = joblib.load(model_path)
            logger.info(f"✓ Model loaded successfully from {model_path}")
            return model
        except (NotImplementedError, AttributeError, TypeError, pickle.UnpicklingError) as e:
            # Cross-version pickle compatibility error (Python version, pandas version, etc.)
            logger.warning(f"⚠ Model pickle incompatible (likely Python/library version mismatch): {type(e).__name__}")
            logger.info("Will retrain model from fresh data on startup")
            return None
        except Exception as e:
            logger.error(f"Unexpected error loading model: {str(e)}", exc_info=True)
            return None

    def retrain_model(self, returns_series):
        """Retrain GARCH model with raw returns data (NOT volatility)
        
        Args:
            returns_series: pandas Series of raw returns (e.g., from df['returns']), NOT pre-computed volatility
        """
        try:
            from arch import arch_model
            
            if returns_series is None or len(returns_series) < 20:
                logger.warning(f"Insufficient data for retraining: {len(returns_series) if returns_series is not None else 0} points")
                return False
            
            logger.info(f"Retraining GARCH model with {len(returns_series)} returns data points...")
            
            # IMPORTANT: We receive raw returns (e.g., 0.0028, 0.0036, ...)
            # Scale by 100 before fitting to convert to percentage scale (GARCH convention)
            data = returns_series.copy()
            
            # Fit new GARCH model with explicit parameters to match notebook
            model_fit = arch_model(data * 100, vol='Garch', p=1, q=1, rescale=False).fit(disp='off')
            
            # Save new model
            model_path = self.model_dir / 'garch_btcusdt_1h.pkl'
            joblib.dump(model_fit, model_path)
            logger.info(f"✓ Model retrained and saved to {model_path}")
            
            # Update in-memory model with lock
            with self._model_lock:
                self.model = model_fit
            logger.debug(f"✓ In-memory model updated")
            
            return True
        except Exception as e:
            logger.error(f"Failed to retrain model: {str(e)}", exc_info=True)
            return False

    def predict_volatility(self):
        """Generate next-step volatility prediction and save to database"""
        if self.model is None:
            logger.error("Model not available - still being trained on startup")
            raise ValueError("Model not available. This typically happens on first startup - wait for model retraining to complete.")
        
        try:
            logger.debug("Generating volatility prediction...")
            prediction_time = datetime.now(timezone.utc)
            
            # Forecast next step with lock (model could be retraining in background)
            with self._model_lock:
                forecast = self.model.forecast(horizon=1)
                # Model was trained on data*100 (percentage scale)
                # Forecast variance is in units of (100*returns)^2 = (10000*returns^2)
                # Convert back to decimal scale: divide by 10000, then sqrt
                conditional_variance = float(forecast.variance.values[-1, 0])
                # This matches notebook formula: pred_var = var/(100^2), then pred_vol = sqrt(pred_var)
                predicted_volatility = np.sqrt(conditional_variance / 10000.0)
                
                logger.info(f"Forecast: raw_variance={conditional_variance:.6f}, scaled_volatility={predicted_volatility:.6f}")
            
            logger.debug(f"Forecast generated: volatility={predicted_volatility:.6f}")
            
            prediction_payload = {
                "prediction_time": prediction_time,
                "predicted_volatility": predicted_volatility,
            }
            
            prediction_id = self.save_prediction(prediction_payload)
            prediction_payload["id"] = prediction_id
            
            logger.info(f"✓ Prediction saved: id={prediction_id}, vol={predicted_volatility:.6f}")
            return prediction_payload
        except Exception as e:
            logger.error(f"Failed to predict volatility: {str(e)}", exc_info=True)
            raise

    def save_prediction(self, prediction):
        """Persist prediction to database"""
        try:
            created_at = datetime.now(timezone.utc)
            with self.SessionLocal() as session:
                prediction_row = Prediction(
                    prediction_time=prediction["prediction_time"],
                    predicted_volatility=prediction["predicted_volatility"],
                    created_at=created_at,
                )
                session.add(prediction_row)
                session.commit()
                session.refresh(prediction_row)
                logger.debug(f"Prediction persisted with id={prediction_row.id}")
                return prediction_row.id
        except Exception as e:
            logger.error(f"Failed to save prediction: {str(e)}", exc_info=True)
            raise

    def get_prediction_history(self, limit=50):
        """Get recent predictions from database"""
        if not 1 <= limit <= 10000:
            logger.warning(f"Invalid limit {limit}, clamping to valid range")
            limit = max(1, min(limit, 10000))
        
        logger.debug(f"Fetching {limit} recent predictions")
        try:
            with self.SessionLocal() as session:
                rows = session.execute(
                    select(Prediction)
                    .order_by(Prediction.id.desc())
                    .limit(limit)
                ).scalars().all()

            predictions = [row.to_dict() for row in rows]
            logger.debug(f"Retrieved {len(predictions)} predictions")
            return predictions
        except Exception as e:
            logger.error(f"Failed to fetch prediction history: {str(e)}", exc_info=True)
            raise

    def get_latest_prediction(self):
        """Get the most recent prediction (next-step forecast)"""
        logger.debug("Fetching latest prediction")
        try:
            with self.SessionLocal() as session:
                row = session.execute(
                    select(Prediction)
                    .order_by(Prediction.id.desc())
                    .limit(1)
                ).scalar()
            
            if row is None:
                logger.debug("No predictions found in database")
                return None
            
            prediction = row.to_dict()
            logger.debug(f"Latest prediction: id={prediction['id']}, vol={prediction['predicted_volatility']:.6f}")
            return prediction
        except Exception as e:
            logger.error(f"Failed to fetch latest prediction: {str(e)}", exc_info=True)
            raise


def get_predictor():
    """Get or create singleton Predictor instance"""
    global _predictor_instance
    if _predictor_instance is None:
        logger.info("Creating new Predictor singleton instance")
        _predictor_instance = Predictor()
    return _predictor_instance

