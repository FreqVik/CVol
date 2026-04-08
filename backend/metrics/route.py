from fastapi import APIRouter, HTTPException
import logging
from .service import get_metrics_service

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post('/compute')
async def compute_metrics(
    symbol: str = 'BTC/USDT',
    timeframe: str = '1h',
    window: int = 20,
    prediction_limit: int = 200,
):
    """Compute and store prediction accuracy metrics"""
    try:
        if not 1 <= window <= 500:
            raise HTTPException(status_code=400, detail="window must be between 1 and 500")
        if not 1 <= prediction_limit <= 10000:
            raise HTTPException(status_code=400, detail="prediction_limit must be between 1 and 10000")
        
        logger.info(f"POST /compute: {symbol} {timeframe} (window={window}, limit={prediction_limit})")
        metrics_service = get_metrics_service()
        result = metrics_service.compute_and_store_metrics(
            symbol=symbol,
            timeframe=timeframe,
            window=window,
            prediction_limit=prediction_limit,
        )
        logger.info(f"✓ Metrics computed: status={result.get('status')}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to compute metrics: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/latest')
async def latest_metrics(symbol: str = 'BTC/USDT', timeframe: str = '1h', window: int = 20):
    """Get most recent metric snapshot"""
    try:
        logger.info(f"GET /latest: {symbol} {timeframe}")
        metrics_service = get_metrics_service()
        result = metrics_service.get_latest_metrics(symbol=symbol, timeframe=timeframe, window=window)
        
        if result is None:
            logger.debug("No metrics found")
            raise HTTPException(status_code=404, detail='No metrics snapshots found for the requested parameters.')
        
        logger.debug(f"✓ Returned latest metrics: id={result['id']}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch latest metrics: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/history')
async def metrics_history(
    symbol: str = 'BTC/USDT',
    timeframe: str = '1h',
    window: int = 20,
    limit: int = 50,
):
    """Get recent metric snapshots"""
    try:
        if not 1 <= limit <= 1000:
            raise HTTPException(status_code=400, detail="limit must be between 1 and 1000")
        
        logger.info(f"GET /history: {symbol} {timeframe} limit={limit}")
        metrics_service = get_metrics_service()
        result = metrics_service.get_metrics_history(
            symbol=symbol,
            timeframe=timeframe,
            window=window,
            limit=limit,
        )
        logger.debug(f"✓ Returned {len(result)} snapshots")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch metrics history: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/prediction-metrics')
async def prediction_metrics(
    symbol: str = 'BTC/USDT',
    timeframe: str = '1h',
    window: int = 20,
    limit: int = 100,
):
    """Get individual prediction-volatility metrics"""
    try:
        if not 1 <= limit <= 10000:
            raise HTTPException(status_code=400, detail="limit must be between 1 and 10000")
        
        logger.info(f"GET /prediction-metrics: {symbol} {timeframe} limit={limit}")
        metrics_service = get_metrics_service()
        result = metrics_service.get_prediction_metrics(
            symbol=symbol,
            timeframe=timeframe,
            window=window,
            limit=limit,
        )
        logger.debug(f"✓ Returned {len(result)} prediction metrics")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch prediction metrics: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
