from fastapi import APIRouter, HTTPException
import logging
from .service import get_predictor

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/predict")
async def predict_volatility():
    """Generate next-step volatility prediction"""
    try:
        logger.info("POST /predict: Generating next-step volatility prediction")
        predictor = get_predictor()
        prediction = predictor.predict_volatility()
        logger.info(f"✓ Prediction generated: id={prediction['id']}, vol={prediction['predicted_volatility']:.6f}")
        return prediction
    except FileNotFoundError as e:
        logger.error(f"Model file not found: {str(e)}")
        raise HTTPException(status_code=503, detail=f"Model not available: {str(e)}")
    except Exception as e:
        logger.error(f"Prediction failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")


@router.get("/latest")
async def get_latest_prediction():
    """Get the most recent (next-step) prediction"""
    try:
        logger.info("GET /latest: Fetching latest prediction")
        predictor = get_predictor()
        prediction = predictor.get_latest_prediction()
        
        if prediction is None:
            logger.warning("No predictions found in database")
            raise HTTPException(status_code=404, detail="No predictions available yet. Call POST /predict first.")
        
        logger.debug(f"✓ Returned latest prediction: id={prediction['id']}")
        return prediction
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch latest prediction: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch prediction: {str(e)}")


@router.get("/predictions")
async def get_predictions(limit: int = 50):
    """Get recent prediction history"""
    try:
        if not 1 <= limit <= 10000:
            logger.warning(f"Invalid limit={limit}, clamping to valid range")
            raise HTTPException(status_code=400, detail="limit must be between 1 and 10000")
        
        logger.info(f"GET /predictions: Fetching {limit} recent predictions")
        predictor = get_predictor()
        predictions = predictor.get_prediction_history(limit=limit)
        logger.debug(f"✓ Returned {len(predictions)} predictions")
        return predictions
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch predictions: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch predictions: {str(e)}")