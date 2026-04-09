import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from chart.route import router as chart_router, chart_service
from metrics.route import router as metrics_router
from metrics.service import get_metrics_service
from predict.route import router as predict_router
from predict.service import get_predictor
from bg.scheduler import start_scheduler, stop_scheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize chart service with 30 days of BTC data
    logger.info("=" * 60)
    logger.info("STARTUP: Initializing CVol Backend...")
    logger.info("=" * 60)
    
    try:
        result = await chart_service.initialize(symbol='BTC/USDT', timeframe='1h', days=30, window=20)
        logger.info(f"✓ Chart service initialized: {result}")
    except Exception as e:
        logger.error(f"✗ Failed to initialize chart service: {str(e)}", exc_info=True)
    
    # Validate predictor model on startup
    try:
        logger.info("Validating predictor model...")
        predictor = get_predictor()
        
        # If model is None (pickle incompatible or missing), retrain from chart data
        if predictor.model is None:
            logger.warning("⚠ Model is None - attempting to retrain from chart data...")
            try:
                df_chart = chart_service.get_data(symbol='BTC/USDT', timeframe='1h')
                if len(df_chart) >= 20:
                    # Use raw returns (NOT realized_vol) for GARCH training
                    returns_series = df_chart['returns'].dropna()
                    success = predictor.retrain_model(returns_series)
                    if success:
                        logger.info("✓ Model retrained from chart data on startup")
                    else:
                        logger.error("Failed to retrain model from chart data")
                else:
                    logger.error("Insufficient chart data to retrain model")
            except Exception as retrain_error:
                logger.error(f"Failed to retrain model: {str(retrain_error)}", exc_info=True)
        else:
            logger.info("✓ Predictor model loaded and validated")
            # Even if model loaded, retrain with latest data on startup for fresh start
            try:
                logger.info("Retraining model with latest data on startup...")
                df_chart = chart_service.get_data(symbol='BTC/USDT', timeframe='1h')
                if len(df_chart) >= 20:
                    returns_series = df_chart['returns'].dropna()
                    success = predictor.retrain_model(returns_series)
                    if success:
                        logger.info("✓ Model retrained with latest data on startup")
                    else:
                        logger.warning("Model retrain with latest data was skipped")
            except Exception as retrain_error:
                logger.warning(f"Could not retrain model on startup (non-fatal): {str(retrain_error)}")
    except Exception as e:
        logger.error(f"✗ Failed to load predictor model: {str(e)}", exc_info=True)
        predictor = None
    
    # Initialize metrics service with chart_service dependency
    try:
        logger.info("Initializing metrics service...")
        metrics_service = get_metrics_service(chart_service=chart_service)
        logger.info(f"✓ Metrics service initialized")
    except Exception as e:
        logger.error(f"✗ Failed to initialize metrics service: {str(e)}", exc_info=True)
        metrics_service = None
    
    # Start background scheduler with full pipeline
    try:
        start_scheduler(
            chart_service=chart_service,
            metrics_service=metrics_service,
            predictor=predictor
        )
        logger.info("✓ Background scheduler started")
    except Exception as e:
        logger.error(f"✗ Failed to start background scheduler: {str(e)}", exc_info=True)
    
    logger.info("=" * 60)
    logger.info("STARTUP COMPLETE: CVol Backend is ready")
    logger.info("=" * 60)
    
    yield
    
    # Shutdown
    logger.info("=" * 60)
    logger.info("SHUTDOWN: Stopping CVol Backend...")
    logger.info("=" * 60)
    stop_scheduler()
    logger.info("✓ Shutdown complete")

app = FastAPI(title='CVol Backend', version='0.1.0', lifespan=lifespan)

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (change to specific domains in production)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, PUT, DELETE, OPTIONS, etc.)
    allow_headers=["*"],  # Allow all headers
)


@app.get('/')
async def root():
	return {
		'message': 'CVol backend is running.',
		'routes': {
			'chart': '/chart',
			'predict': '/predict',
			'metrics': '/metrics',
			'health': '/health',
		},
	}


@app.get('/health')
async def health():
	return {'status': 'ok'}


app.include_router(chart_router, prefix='/chart', tags=['chart'])
app.include_router(predict_router, prefix='/predict', tags=['predict'])
app.include_router(metrics_router, prefix='/metrics', tags=['metrics'])


if __name__ == '__main__':
	import uvicorn

	uvicorn.run('main:app', host='0.0.0.0', port=8000, reload=True)
