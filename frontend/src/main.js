import { createApp } from 'vue'
import App from './App.vue'
import './assets/styles.css'

console.log('✓ main.js loading')

const app = createApp(App)
console.log('✓ Vue app created')

app.mount('#app')
console.log('✓ Vue app mounted to #app')
