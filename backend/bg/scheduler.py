"""Background scheduler for periodic chart data updates using APScheduler"""
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()


def refresh_btc_chart_data(chart_service):
    """Refresh BTC/USDT 1h chart data every hour"""
    try:
        logger.info("Starting scheduled chart data refresh for BTC/USDT 1h...")
        df, appended = chart_service.append_new_data(symbol='BTC/USDT', timeframe='1h', window=20)
        logger.info(f"✓ Chart refresh complete. Appended {appended} rows. Total: {len(df)} rows")
    except Exception as e:
        logger.error(f"✗ Failed to refresh chart data: {str(e)}", exc_info=True)


def compute_metrics_snapshot(metrics_service):
    """Compute and store metrics after chart refresh"""
    try:
        logger.info("Starting scheduled metrics computation for BTC/USDT 1h...")
        result = metrics_service.compute_and_store_metrics(
            symbol='BTC/USDT',
            timeframe='1h',
            window=20,
            prediction_limit=200
        )
        if result.get('status') == 'completed':
            logger.info(f"✓ Metrics computed: {result['prediction_count']} pairs, MAE={result['mae']:.6f}")
        else:
            logger.debug(f"Metrics skipped: {result.get('message')}")
    except Exception as e:
        logger.error(f"✗ Failed to compute metrics: {str(e)}", exc_info=True)


def generate_next_prediction(predictor):
    """Generate next-step prediction after metrics"""
    try:
        logger.info("Generating next-step prediction...")
        prediction = predictor.predict_volatility()
        logger.info(f"✓ Prediction generated: vol={prediction['predicted_volatility']:.6f}")
    except Exception as e:
        logger.error(f"✗ Failed to generate prediction: {str(e)}", exc_info=True)


def retrain_model_background(metrics_service):
    """Background job: Retrain GARCH model with latest realized volatility"""
    try:
        logger.info("Starting background model retraining...")
        success = metrics_service._trigger_model_retrain(symbol='BTC/USDT', timeframe='1h')
        if success:
            logger.info(f"✓ Model retraining completed successfully")
        else:
            logger.warning("Model retraining was skipped or failed")
    except Exception as e:
        logger.error(f"✗ Failed to retrain model: {str(e)}", exc_info=True)


def start_scheduler(chart_service, metrics_service=None, predictor=None):
    """Start the background scheduler with chart refresh → metrics → prediction pipeline"""
    if scheduler.running:
        logger.warning("Scheduler already running")
        return
    
    # Every 1 hour: refresh chart data
    scheduler.add_job(
        func=refresh_btc_chart_data,
        args=[chart_service],
        trigger=IntervalTrigger(hours=1),
        id='refresh_btc_chart_1h',
        name='Refresh BTC/USDT 1h chart data',
        replace_existing=True,
        coalesce=True,
        max_instances=1
    )
    logger.debug("Added job: refresh_btc_chart_1h (0:00)")
    
    # After chart refresh (30 seconds later): compute metrics
    if metrics_service:
        scheduler.add_job(
            func=compute_metrics_snapshot,
            args=[metrics_service],
            trigger=IntervalTrigger(hours=1, minutes=0, seconds=30),
            id='compute_metrics_snapshot',
            name='Compute metrics snapshot',
            replace_existing=True,
            coalesce=True,
            max_instances=1
        )
        logger.debug("Added job: compute_metrics_snapshot (0:30)")
    
    # Background job (90 seconds after chart): Retrain model in background (doesn't block prediction)
    if metrics_service:
        scheduler.add_job(
            func=retrain_model_background,
            args=[metrics_service],
            trigger=IntervalTrigger(hours=1, minutes=1, seconds=30),
            id='retrain_model_background',
            name='Retrain GARCH model (background)',
            replace_existing=True,
            coalesce=True,
            max_instances=1
        )
        logger.debug("Added job: retrain_model_background (1:30, async)")
    
    # After metrics (60 seconds after chart refresh): generate prediction
    if predictor:
        scheduler.add_job(
            func=generate_next_prediction,
            args=[predictor],
            trigger=IntervalTrigger(hours=1, minutes=1),
            id='generate_next_prediction',
            name='Generate next-step prediction',
            replace_existing=True,
            coalesce=True,
            max_instances=1
        )
        logger.debug("Added job: generate_next_prediction (1:00)")
    
    scheduler.start()
    logger.info("Background scheduler started: chart(0:00) → metrics(0:30) → prediction(1:00) ∥ retrain(1:30, async)")


def stop_scheduler():
    """Stop the background scheduler"""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Background scheduler stopped.")
