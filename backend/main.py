from fastapi import FastAPI

from chart.route import router as chart_router
from metrics.route import router as metrics_router
from predict.route import router as predict_router


app = FastAPI(title='CVol Backend', version='0.1.0')


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
