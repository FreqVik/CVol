from functools import lru_cache

from fastapi import APIRouter, HTTPException

from .service import MetricsService

router = APIRouter()


@lru_cache(maxsize=1)
def get_metrics_service() -> MetricsService:
    return MetricsService()


@router.post('/compute')
async def compute_metrics(
    symbol: str = 'BTC/USDT',
    timeframe: str = '4h',
    window: int = 20,
    prediction_limit: int = 200,
):
    try:
        metrics_service = get_metrics_service()
        return metrics_service.compute_and_store_metrics(
            symbol=symbol,
            timeframe=timeframe,
            window=window,
            prediction_limit=prediction_limit,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/latest')
async def latest_metrics(symbol: str = 'BTC/USDT', timeframe: str = '4h', window: int = 20):
    try:
        metrics_service = get_metrics_service()
        result = metrics_service.get_latest_metrics(symbol=symbol, timeframe=timeframe, window=window)
        if result is None:
            return {
                'message': 'No metrics snapshots found for the requested parameters.'
            }
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/history')
async def metrics_history(
    symbol: str = 'BTC/USDT',
    timeframe: str = '4h',
    window: int = 20,
    limit: int = 50,
):
    try:
        metrics_service = get_metrics_service()
        return metrics_service.get_metrics_history(
            symbol=symbol,
            timeframe=timeframe,
            window=window,
            limit=limit,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/prediction-metrics')
async def prediction_metrics(
    symbol: str = 'BTC/USDT',
    timeframe: str = '4h',
    window: int = 20,
    limit: int = 100,
):
    try:
        metrics_service = get_metrics_service()
        return metrics_service.get_prediction_metrics(
            symbol=symbol,
            timeframe=timeframe,
            window=window,
            limit=limit,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
