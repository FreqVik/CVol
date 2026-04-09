<template>
  <div class="bg-white rounded-lg shadow-md p-6 h-full flex flex-col">
    <h2 class="text-2xl font-bold text-gray-800 mb-6">Metrics & Prediction</h2>

    <!-- Prediction Card -->
    <div class="bg-gradient-to-br from-blue-50 to-indigo-50 rounded-lg p-6 mb-6 border-2 border-primary">
      <p class="text-gray-600 text-sm mb-2">Next Prediction</p>
      <div v-if="loadingPrediction" class="h-10 bg-gray-300 rounded animate-pulse"></div>
      <div v-else-if="prediction" class="flex items-baseline gap-3">
        <span class="text-4xl font-bold text-primary">{{ prediction.predicted_volatility.toFixed(6) }}</span>
        <span :class="predictionChange > 0 ? 'text-success' : 'text-danger'" class="text-lg font-semibold">
          {{ predictionChange > 0 ? '↑' : '↓' }} {{ Math.abs(predictionChange).toFixed(2) }}%
        </span>
      </div>
      <p v-else class="text-gray-500">No prediction available</p>
      <p class="text-xs text-gray-500 mt-2">
        {{ prediction ? new Date(prediction.prediction_time).toLocaleString() : 'N/A' }}
      </p>
    </div>

    <!-- Metrics Grid -->
    <div class="grid grid-cols-2 gap-4 mb-6">
      <MetricCard
        label="Accuracy"
        :value="metrics?.accuracy ? (metrics.accuracy * 100).toFixed(2) + '%' : 'N/A'"
        :loading="loadingMetrics"
      />
      <MetricCard
        label="Win Rate"
        :value="metrics?.win_rate ? (metrics.win_rate * 100).toFixed(2) + '%' : 'N/A'"
        :loading="loadingMetrics"
      />
      <MetricCard
        label="MAE"
        :value="metrics?.mae ? metrics.mae.toFixed(6) : 'N/A'"
        :loading="loadingMetrics"
      />
      <MetricCard
        label="RMSE"
        :value="metrics?.rmse ? metrics.rmse.toFixed(6) : 'N/A'"
        :loading="loadingMetrics"
      />
    </div>

    <!-- Detailed Metrics -->
    <div class="bg-gray-50 rounded-lg p-4 mb-6 flex-grow">
      <h3 class="font-semibold text-gray-800 mb-3">Performance Metrics</h3>
      
      <div v-if="loadingMetrics" class="space-y-2">
        <div class="h-4 bg-gray-300 rounded animate-pulse w-3/4"></div>
        <div class="h-4 bg-gray-300 rounded animate-pulse w-1/2"></div>
        <div class="h-4 bg-gray-300 rounded animate-pulse w-2/3"></div>
      </div>

      <div v-else-if="metrics" class="space-y-2 text-sm">
        <div class="flex justify-between">
          <span class="text-gray-600">Sharpe Ratio:</span>
          <span class="font-semibold text-gray-800">{{ metrics.sharpe_ratio?.toFixed(3) || 'N/A' }}</span>
        </div>
        <div class="flex justify-between">
          <span class="text-gray-600">Total Predictions:</span>
          <span class="font-semibold text-gray-800">{{ metrics.total_predictions || 0 }}</span>
        </div>
        <div class="flex justify-between">
          <span class="text-gray-600">Correct Predictions:</span>
          <span class="font-semibold text-gray-800 text-success">{{ metrics.correct_predictions || 0 }}</span>
        </div>
        <div class="flex justify-between">
          <span class="text-gray-600">Mean Error:</span>
          <span class="font-semibold text-gray-800">{{ metrics.mean_error?.toFixed(6) || 'N/A' }}</span>
        </div>
      </div>
      <div v-else class="text-gray-500 text-center py-4">No metrics available</div>
    </div>

    <!-- Error Message -->
    <div v-if="error" class="bg-red-100 text-red-700 p-3 rounded mb-4 text-sm">
      {{ error }}
    </div>

    <!-- Action Buttons -->
    <div class="flex gap-2">
      <button
        @click="generatePrediction"
        :disabled="loadingPrediction"
        class="flex-1 px-4 py-2 bg-success text-white rounded hover:bg-green-600 disabled:opacity-50 transition font-semibold"
      >
        {{ loadingPrediction ? 'Predicting...' : '⭐ New Prediction' }}
      </button>
      <button
        @click="refreshMetrics"
        :disabled="loadingMetrics"
        class="flex-1 px-4 py-2 bg-primary text-white rounded hover:bg-blue-700 disabled:opacity-50 transition font-semibold"
      >
        {{ loadingMetrics ? 'Computing...' : 'Refresh' }}
      </button>
    </div>

    <!-- Auto-refresh Info -->
    <p class="text-xs text-gray-500 mt-3 text-center">
      Auto-refreshes every {{ Math.round(refreshInterval / 60000) }}min
    </p>
  </div>
</template>

<script setup>
import { ref, onMounted, computed, onBeforeUnmount } from 'vue'
import api from '../services/api'
import MetricCard from './MetricCard.vue'

const prediction = ref(null)
const metrics = ref(null)
const loadingPrediction = ref(false)
const loadingMetrics = ref(false)
const error = ref('')
const lastPredictionValue = ref(null)
const refreshInterval = ref(300000)  // 5 minutes = 300,000ms

let metricsRefreshTimer = null
let predictionRefreshTimer = null

const predictionChange = computed(() => {
  if (!prediction.value || !lastPredictionValue.value) return 0
  return ((prediction.value.predicted_volatility - lastPredictionValue.value) / lastPredictionValue.value) * 100
})

const loadPrediction = async () => {
  loadingPrediction.value = true
  error.value = ''
  try {
    const response = await api.getLatestPrediction()
    if (lastPredictionValue.value === null) {
      lastPredictionValue.value = response.data.predicted_volatility
    }
    prediction.value = response.data
  } catch (err) {
    if (err.response?.status !== 404) {
      error.value = 'Failed to load prediction'
      console.error(err)
    }
  } finally {
    loadingPrediction.value = false
  }
}

const loadMetrics = async () => {
  loadingMetrics.value = true
  error.value = ''
  try {
    const response = await api.getLatestMetrics('BTC/USDT', '1h', 20)
    metrics.value = response.data
  } catch (err) {
    if (err.response?.status !== 404) {
      error.value = 'Failed to load metrics'
      console.error(err)
    }
  } finally {
    loadingMetrics.value = false
  }
}

const generatePrediction = async () => {
  loadingPrediction.value = true
  error.value = ''
  try {
    const response = await api.generatePrediction()
    lastPredictionValue.value = prediction.value?.predicted_volatility
    prediction.value = response.data
    // Refresh metrics after new prediction
    await loadMetrics()
  } catch (err) {
    error.value = 'Failed to generate prediction'
    console.error(err)
  } finally {
    loadingPrediction.value = false
  }
}

const refreshMetrics = async () => {
  // Compute new metrics
  loadingMetrics.value = true
  error.value = ''
  try {
    await api.computeMetrics('BTC/USDT', '1h', 20, 200)
    // Then load the updated metrics
    await loadMetrics()
  } catch (err) {
    error.value = 'Failed to compute metrics'
    console.error(err)
  } finally {
    loadingMetrics.value = false
  }
}

const setupAutoRefresh = () => {
  // Auto-refresh metrics
  metricsRefreshTimer = setInterval(() => {
    loadMetrics()
  }, refreshInterval.value)

  // Auto-refresh prediction
  predictionRefreshTimer = setInterval(() => {
    loadPrediction()
  }, refreshInterval.value)
}

onMounted(() => {
  loadPrediction()
  loadMetrics()
  setupAutoRefresh()
})

onBeforeUnmount(() => {
  if (metricsRefreshTimer) clearInterval(metricsRefreshTimer)
  if (predictionRefreshTimer) clearInterval(predictionRefreshTimer)
})
</script>
