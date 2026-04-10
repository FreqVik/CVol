<template>
  <div class="bg-white rounded-lg shadow-md p-6 h-full">
    <div class="mb-4 flex justify-between items-center">
      <h2 class="text-2xl font-bold text-gray-800">Volatility Chart</h2>
      <div class="flex gap-2">
        <div class="flex items-center gap-2">
          <label class="text-sm text-gray-600">Show last:</label>
          <select v-model.number="dataPoints" class="px-2 py-1 border rounded text-sm">
            <option :value="20">20</option>
            <option :value="30">30</option>
            <option :value="50">50</option>
            <option :value="100">100</option>
          </select>
        </div>
        <button
          @click="refreshChart"
          :disabled="loading"
          class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 transition"
        >
          {{ loading ? 'Loading...' : 'Refresh' }}
        </button>
      </div>
    </div>

    <!-- Error Message -->
    <div v-if="error" class="bg-red-100 text-red-700 p-3 rounded mb-4 text-sm">
      {{ error }}
    </div>

    <!-- Loading State -->
    <div v-if="loading" class="w-full bg-gray-200 rounded animate-pulse" style="height: 400px;"></div>

    <!-- Chart Container -->
    <div v-else class="relative w-full">
      <div v-if="chartData.length === 0" class="w-full text-center py-20 text-gray-500">
        <p>No chart data available</p>
      </div>
      <div v-else class="chart-wrapper">
        <canvas ref="chartCanvas"></canvas>
      </div>

      <!-- Legend -->
      <div class="mt-6 text-sm text-gray-600 flex gap-8 justify-center flex-wrap">
        <span class="inline-flex items-center gap-2">
          <span class="inline-block w-3 h-3 bg-blue-600 rounded"></span>
          Historical Volatility
        </span>
        <span v-if="prediction" class="inline-flex items-center gap-2">
          <span class="inline-block w-3 h-3 bg-black rounded border-2 border-yellow-400"></span>
          Predicted: {{ prediction.predicted_volatility.toFixed(6) }}
        </span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, nextTick, watch } from 'vue'
import Chart from 'chart.js/auto'
import api from '../services/api'

const chartCanvas = ref(null)
const chart = ref(null)
const loading = ref(false)
const error = ref('')
const chartData = ref([])
const prediction = ref(null)
const dataPoints = ref(30)

let autoRefreshTimer = null
const AUTO_REFRESH_INTERVAL = 300000  // 5 minutes

watch(dataPoints, () => {
  if (chartData.value.length > 0) {
    renderChart()
  }
})

const loadChartData = async () => {
  loading.value = true
  error.value = ''
  try {
    console.log('📊 Fetching chart data...')
    const response = await api.getChartData('BTC/USDT', '1h', 100)
    console.log('✓ Chart data received:', response.data.length, 'records')
    
    if (!response.data || response.data.length === 0) {
      error.value = 'No data returned from server'
      loading.value = false
      return
    }
    
    chartData.value = response.data
    console.log('✓ Chart data set in state')

    // Fetch prediction
    try {
      const predResponse = await api.getLatestPrediction()
      prediction.value = predResponse.data
      console.log('✓ Prediction fetched:', prediction.value)
      console.log('  - Value:', prediction.value.predicted_volatility)
      console.log('  - Time:', prediction.value.prediction_time)
    } catch (e) {
      console.warn('⚠️  Could not fetch prediction:', e.message)
      prediction.value = null
    }
    
    // Set loading to false so canvas renders
    loading.value = false
    console.log('✓ Loading set to false')
    
    // Wait for Vue to render the canvas to DOM
    await nextTick()
    console.log('✓ DOM updated, canvas should be ready')
    
    // Now render the chart
    renderChart()
  } catch (err) {
    error.value = `Failed to load chart data: ${err.message}`
    console.error('❌ Chart load error:', err)
    loading.value = false
  }
}

const renderChart = () => {
  console.log('🎨 Rendering chart')
  
  if (!chartCanvas.value) {
    console.error('❌ Canvas ref not available')
    return
  }

  if (chartData.value.length === 0) {
    console.error('❌ No chart data to render')
    return
  }

  try {
    // Destroy existing chart
    if (chart.value) {
      chart.value.destroy()
    }

    // Get only the last N data points
    const recentData = chartData.value.slice(-dataPoints.value)
    
    const labels = recentData.map(d => {
      const date = new Date(d.timestamp)
      return date.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit' })
    })

    const volatilityData = recentData.map(d => parseFloat(d.realized_vol))
    
    console.log('📊 Prediction available:', prediction.value)
    if (prediction.value) {
      console.log('🎯 Predicted volatility:', prediction.value.predicted_volatility)
    }
    
    // Create prediction dataset - just a single point
    const predictionDataset = {
      label: 'Predicted Volatility',
      data: Array(volatilityData.length + 1).fill(null),
      borderColor: '#000000',
      pointRadius: 0,
      pointBackgroundColor: '#000000',
      pointBorderColor: '#FFD700',
      pointBorderWidth: 3,
      borderWidth: 0,
      fill: false,
      tension: 0,
    }

    console.log('Chart data points:', volatilityData.length)
    console.log('Volatility range:', Math.min(...volatilityData).toFixed(4), 'to', Math.max(...volatilityData).toFixed(4))

    const ctx = chartCanvas.value.getContext('2d')
    
    const datasets = [
      {
        label: 'Realized Volatility',
        data: volatilityData,
        borderColor: '#1e40af',
        backgroundColor: 'rgba(30, 64, 175, 0.05)',
        tension: 0.4,
        fill: true,
        pointRadius: 4,
        pointBackgroundColor: '#1e40af',
        pointBorderColor: '#fff',
        pointBorderWidth: 1,
        pointHoverRadius: 6,
        borderWidth: 2,
      }
    ]

    // Add prediction point if available
    if (prediction.value && prediction.value.predicted_volatility) {
      // Set the last point as the prediction
      predictionDataset.data[volatilityData.length] = parseFloat(prediction.value.predicted_volatility)
      // Use pointRadius as a function to only show large point at the last index
      predictionDataset.pointRadius = function(context) {
        return context.dataIndex === volatilityData.length ? 10 : 0
      }
      predictionDataset.pointHoverRadius = function(context) {
        return context.dataIndex === volatilityData.length ? 15 : 0
      }
      
      datasets.push(predictionDataset)
      
      // Add label for prediction
      labels.push('Prediction')
      
      console.log('✓ Prediction added to plot at index', volatilityData.length)
      console.log('  - Value:', prediction.value.predicted_volatility)
    } else {
      console.warn('⚠️  No prediction value to plot')
    }
    
    chart.value = new Chart(ctx, {
      type: 'line',
      data: {
        labels,
        datasets
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        layout: {
          padding: 10
        },
        interaction: {
          intersect: false,
          mode: 'index'
        },
        plugins: {
          legend: {
            display: true,
            position: 'top',
            labels: {
              usePointStyle: true,
              padding: 15,
              font: { size: 12, weight: 'bold' }
            }
          },
          tooltip: {
            backgroundColor: 'rgba(0,0,0,0.8)',
            titleColor: '#fff',
            bodyColor: '#fff',
            borderColor: '#1e40af',
            borderWidth: 1,
            padding: 10,
            callbacks: {
              label: function(context) {
                let label = context.dataset.label || ''
                if (label) {
                  label += ': '
                }
                if (context.parsed.y !== null) {
                  label += context.parsed.y.toFixed(6)
                }
                return label
              }
            }
          }
        },
        scales: {
          y: {
            beginAtZero: false,
            ticks: {
              callback: function(value) {
                return value.toFixed(4)
              }
            },
            grid: {
              color: 'rgba(200, 200, 200, 0.1)'
            }
          },
          x: {
            grid: {
              color: 'rgba(200, 200, 200, 0.1)'
            }
          }
        }
      }
    })

    console.log('✓ Chart rendered successfully')
  } catch (chartErr) {
    console.error('❌ Chart rendering error:', chartErr)
    error.value = `Chart rendering error: ${chartErr.message}`
  }
}

const refreshChart = async () => {
  await loadChartData()
}

const updatePredictionOnly = async () => {
  try {
    const predResponse = await api.getLatestPrediction()
    prediction.value = predResponse.data
    console.log('✓ Prediction auto-updated:', prediction.value.predicted_volatility)
    
    // Re-render chart with new prediction
    if (chartData.value.length > 0) {
      renderChart()
    }
  } catch (e) {
    console.log('ℹ️  Could not update prediction:', e.message)
  }
}

const setupAutoRefresh = () => {
  autoRefreshTimer = setInterval(async () => {
    console.log('🔄 Auto-refreshing prediction...')
    await updatePredictionOnly()
  }, AUTO_REFRESH_INTERVAL)
}

onMounted(() => {
  console.log('📦 VolatilityChart component mounted')
  loadChartData()
  setupAutoRefresh()
})

onBeforeUnmount(() => {
  if (chart.value) {
    chart.value.destroy()
  }
  if (autoRefreshTimer) {
    clearInterval(autoRefreshTimer)
  }
})
</script>

<style scoped>
.chart-wrapper {
  position: relative;
  width: 100%;
  height: 400px;
  margin-bottom: 20px;
}

canvas {
  display: block;
  width: 100% !important;
  height: 100% !important;
}

select {
  border-color: #d1d5db;
  background-color: #f9fafb;
}

select:hover {
  border-color: #1e40af;
}
</style>
