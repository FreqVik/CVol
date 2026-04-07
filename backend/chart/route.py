from fastapi import APIRouter
from .service import ChartService

router = APIRouter()
chart_service = ChartService()

@router.get("/returns")
async def get_chart_returns(symbol: str = "BTC/USDT", timeframe: str = "1h", limit: int = 100):
    df = chart_service.get_or_create_dataframe(symbol=symbol, timeframe=timeframe, limit=max(limit, 100))
    df, _ = chart_service.append_new_data(symbol=symbol, timeframe=timeframe)
    result = chart_service.get_returns(df.tail(limit)).dropna(subset=["returns"])
    return result.to_dict(orient="records")

@router.get("/realized-vol")
async def get_realized_vol(symbol: str = "BTC/USDT", timeframe: str = "1h", limit: int = 100, window: int = 20):
    df = chart_service.get_or_create_dataframe(symbol=symbol, timeframe=timeframe, limit=max(limit, 100))
    df, _ = chart_service.append_new_data(symbol=symbol, timeframe=timeframe)
    result = chart_service.compute_realized_vol(df.tail(limit), window=window).dropna(subset=["realized_vol"])
    return result.to_dict(orient="records")

@router.post("/append")
async def append_chart_data(symbol: str = "BTC/USDT", timeframe: str = "1h", fetch_limit: int = 10):
    df, appended = chart_service.append_new_data(symbol=symbol, timeframe=timeframe, fetch_limit=fetch_limit)
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "appended_rows": appended,
        "total_rows": len(df),
    }
