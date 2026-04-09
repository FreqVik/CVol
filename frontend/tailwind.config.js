/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{vue,js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        primary: '#1e40af',
        success: '#16a34a',
        danger: '#dc2626',
        warning: '#ea580c',
      }
    },
  },
  plugins: [],
}
