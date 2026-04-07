from fastapi import APIRouter, HTTPException
from functools import lru_cache
from .service import Predictor

router = APIRouter()

@lru_cache(maxsize=1)
def get_predict_service() -> Predictor:
    return Predictor()

@router.post("/predict")
async def predict_volatility():
    try:
        predict_service = get_predict_service()
        volatility = predict_service.predict_volatility()
        return volatility
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/predictions")
async def get_predictions(limit: int = 50):
    try:
        predict_service = get_predict_service()
        return predict_service.get_prediction_history(limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))