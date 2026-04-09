<template>
  <div class="bg-gray-50 rounded-lg p-4 border border-gray-200 hover:border-primary transition">
    <p class="text-xs text-gray-600 uppercase tracking-wide mb-1">{{ label }}</p>
    <div v-if="loading" class="h-8 bg-gray-300 rounded animate-pulse"></div>
    <p v-else class="text-xl font-bold text-primary">{{ value }}</p>
  </div>
</template>

<script setup>
defineProps({
  label: {
    type: String,
    required: true
  },
  value: {
    type: [String, Number],
    required: true
  },
  loading: {
    type: Boolean,
    default: false
  }
})
</script>
