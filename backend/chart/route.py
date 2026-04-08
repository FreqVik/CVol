from fastapi import APIRouter, HTTPException
from .service import ChartService
import logging

logger = logging.getLogger(__name__)
router = APIRouter()
chart_service = ChartService()

def _validate_params(symbol: str, timeframe: str, limit: int, window: int):
    """Validate input parameters"""
    if not 1 <= limit <= 10000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 10000")
    if not 1 <= window <= 500:
        raise HTTPException(status_code=400, detail="window must be between 1 and 500")
    if timeframe not in ['1h', '4h', '1d', '1w']:
        raise HTTPException(status_code=400, detail="timeframe must be one of: 1h, 4h, 1d, 1w")

@router.get("/data")
async def get_chart_data(symbol: str = "BTC/USDT", timeframe: str = "1h", limit: int = 100):
    """Get OHLCV data with returns and realized volatility"""
    try:
        logger.info(f"GET /data: symbol={symbol}, timeframe={timeframe}, limit={limit}")
        _validate_params(symbol, timeframe, limit, 20)
        df = chart_service.get_data(symbol=symbol, timeframe=timeframe, limit=limit)
        
        if df.empty:
            logger.warning(f"Empty DataFrame returned for {symbol}/{timeframe}")
            return []
        
        # Return only non-null rows with returns and realized_vol
        result = df.dropna(subset=['returns', 'realized_vol'])[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'returns', 'realized_vol']]
        
        result['timestamp'] = result['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        logger.debug(f"✓ Returning {len(result)} records")
        return result.to_dict(orient="records")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_chart_data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/returns")
async def get_chart_returns(symbol: str = "BTC/USDT", timeframe: str = "1h", limit: int = 100):
    """Get price returns (deprecated - use /data instead)"""
    try:
        _validate_params(symbol, timeframe, limit, 20)
        df = chart_service.get_data(symbol=symbol, timeframe=timeframe, limit=limit)
        result = df.dropna(subset=["returns"])[["timestamp", "close", "returns"]]
        result['timestamp'] = result['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        return result.to_dict(orient="records")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_chart_returns: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/realized-vol")
async def get_realized_vol(symbol: str = "BTC/USDT", timeframe: str = "1h", limit: int = 100, window: int = 20):
    """Get realized volatility (deprecated - use /data instead)"""
    try:
        _validate_params(symbol, timeframe, limit, window)
        df = chart_service.get_data(symbol=symbol, timeframe=timeframe, limit=limit)
        result = df.dropna(subset=["realized_vol"])[["timestamp", "close", "realized_vol"]]
        result['timestamp'] = result['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        return result.to_dict(orient="records")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_realized_vol: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/refresh")
async def refresh_chart_data(symbol: str = "BTC/USDT", timeframe: str = "1h"):
    """Fetch new data and append to existing DataFrame (trim to 30 days)"""
    try:
        logger.info(f"POST /refresh: symbol={symbol}, timeframe={timeframe}")
        if timeframe not in ['1h', '4h', '1d', '1w']:
            raise HTTPException(status_code=400, detail="timeframe must be one of: 1h, 4h, 1d, 1w")
        
        df, appended = chart_service.append_new_data(symbol=symbol, timeframe=timeframe, window=20)
        logger.info(f"✓ Refresh endpoint returned: appended={appended}, total={len(df)}")
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "appended_rows": appended,
            "total_rows": len(df),
            "latest_timestamp": df['timestamp'].iloc[-1].isoformat() if not df.empty else None,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in refresh_chart_data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
