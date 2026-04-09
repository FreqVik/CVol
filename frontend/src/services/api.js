import axios from 'axios'

const API_BASE = 'http://localhost:8000'

console.log('📡 API service loading, base URL:', API_BASE)

const api = axios.create({
  baseURL: API_BASE,
  timeout: 10000,
})

export default {
  // Chart endpoints
  getChartData(symbol = 'BTC/USDT', timeframe = '1h', limit = 100) {
    return api.get('/chart/data', {
      params: { symbol, timeframe, limit }
    })
  },

  // Prediction endpoints
  generatePrediction() {
    return api.post('/predict', {})
  },

  getLatestPrediction() {
    return api.get('/predict/latest')
  },

  getPredictionHistory(limit = 50) {
    return api.get('/predict/predictions', {
      params: { limit }
    })
  },

  // Metrics endpoints
  getLatestMetrics(symbol = 'BTC/USDT', timeframe = '1h', window = 20) {
    return api.get('/metrics/latest', {
      params: { symbol, timeframe, window }
    })
  },

  computeMetrics(symbol = 'BTC/USDT', timeframe = '1h', window = 20, predictionLimit = 200) {
    return api.post('/metrics/compute', {}, {
      params: { symbol, timeframe, window, prediction_limit: predictionLimit }
    })
  },
}
