<template>
  <div class="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100 p-4 md:p-8">
    <!-- Header -->
    <div class="max-w-7xl mx-auto mb-8">
      <div class="bg-white rounded-lg shadow-md p-6">
        <div class="flex items-center justify-between">
          <div>
            <h1 class="text-4xl font-bold text-gray-900">📊 CVol Dashboard</h1>
            <p class="text-gray-600 mt-2">BTC/USDT Volatility Prediction & Analysis</p>
          </div>
          <div class="text-right">
            <p class="text-sm text-gray-600">Last Updated</p>
            <p class="text-lg font-semibold text-gray-800">{{ currentTime }}</p>
          </div>
        </div>
      </div>
    </div>

    <!-- Main Content Grid -->
    <div class="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-3 gap-8">
      <!-- Chart Section (Left/Full) -->
      <div class="lg:col-span-2">
        <VolatilityChart />
      </div>

      <!-- Metrics Sidebar (Right) -->
      <div class="lg:col-span-1">
        <MetricsSidebar />
      </div>
    </div>

    <!-- Footer -->
    <div class="max-w-7xl mx-auto mt-8 text-center text-gray-600 text-sm">
      <p>CVol v1.0 • GARCH Volatility Forecasting Model</p>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue'
import VolatilityChart from './components/VolatilityChart.vue'
import MetricsSidebar from './components/MetricsSidebar.vue'

const currentTime = ref('')

console.log('🔥 App.vue is loading!')

const updateTime = () => {
  const now = new Date()
  currentTime.value = now.toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  })
}

let timeInterval = null

onMounted(() => {
  console.log('🚀 App.vue mounted!')
  updateTime()
  timeInterval = setInterval(updateTime, 1000)
})

onBeforeUnmount(() => {
  if (timeInterval) clearInterval(timeInterval)
})
</script>

<style>
body {
  margin: 0;
  padding: 0;
}
</style>
