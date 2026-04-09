# CVol Frontend

Vue 3 + Vite frontend for BTC/USDT volatility prediction dashboard.

## Setup

```bash
cd frontend
npm install
```

## Development

```bash
npm run dev
```

The frontend will run on `http://localhost:5173` and proxy API requests to `http://localhost:8000`.

## Build

```bash
npm run build
```

Production build is output to `dist/`.

## Project Structure

```
frontend/
├── src/
│   ├── components/
│   │   ├── VolatilityChart.vue    # Main volatility chart
│   │   ├── MetricsSidebar.vue     # Metrics and prediction panel
│   │   └── MetricCard.vue         # Reusable metric card
│   ├── services/
│   │   └── api.js                 # API client
│   ├── assets/
│   │   └── styles.css             # Tailwind + custom styles
│   ├── App.vue                    # Root component
│   └── main.js                    # Entry point
├── public/
│   └── index.html
├── package.json
├── vite.config.js
└── tailwind.config.js
```

## Features

- 📈 Interactive volatility chart with Chart.js
- ⭐ Next prediction indicator
- 📊 Real-time metrics display (Accuracy, MAE, RMSE, Sharpe, Win Rate)
- 🔄 Auto-refresh every 5 seconds
- 💾 API integration with FastAPI backend
- 🎨 Responsive Tailwind CSS design
- 📱 Mobile-friendly layout

## API Endpoints Used

- `GET /chart/data` - Historical volatility data
- `GET /predict/latest` - Latest prediction
- `POST /predict` - Generate new prediction
- `GET /metrics/latest` - Latest metrics
- `POST /metrics/compute` - Compute new metrics
