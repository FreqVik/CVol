import joblib
import os
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select

from .model import (
    DEFAULT_DB_PATH,
    Prediction,
    get_prediction_session_factory,
    init_prediction_db,
)

BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_MODEL_DIR = BASE_DIR / 'model'
DEFAULT_DB_PATH = BASE_DIR / 'backend' / 'data' / 'predictions.db'

class Predictor:
    def __init__(self, model_dir=None, db_path=None):
        self.model_dir = Path(model_dir) if model_dir else DEFAULT_MODEL_DIR
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        init_prediction_db(self.db_path)
        self.SessionLocal = get_prediction_session_factory(self.db_path)
        self.model = self.load_model()

    def load_model(self):
        model_path = os.path.join(str(self.model_dir), 'garch_model.pkl')
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model file not found at {model_path}")
        return joblib.load(model_path)

    def predict_volatility(self):
        if self.model is None:
            raise ValueError("Model is not loaded.")
        prediction_time = datetime.now(timezone.utc)
        predicted_volatility = self.model.forecast(horizon=1).variance.values[-1, 0]
        prediction_payload = {
            "prediction_time": prediction_time.isoformat(),
            "predicted_volatility": float(predicted_volatility),
        }
        prediction_id = self.save_prediction(prediction_payload)
        prediction_payload["id"] = prediction_id
        return prediction_payload

    def save_prediction(self, prediction):
        created_at = datetime.now(timezone.utc).isoformat()
        with self.SessionLocal() as session:
            prediction_row = Prediction(
                prediction_time=prediction["prediction_time"],
                predicted_volatility=prediction["predicted_volatility"],
                created_at=created_at,
            )
            session.add(prediction_row)
            session.commit()
            session.refresh(prediction_row)
            return prediction_row.id

    def get_prediction_history(self, limit=50):
        with self.SessionLocal() as session:
            rows = session.execute(
                select(Prediction)
                .order_by(Prediction.id.desc())
                .limit(limit)
            ).scalars().all()

        return [
            {
                "id": row.id,
                "prediction_time": row.prediction_time,
                "predicted_volatility": row.predicted_volatility,
                "created_at": row.created_at,
            }
            for row in rows
        ]
