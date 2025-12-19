/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{html,js}"],
  theme: {
    extend: {
      colors: {
        'mirdb-primary': '#2563eb',
        'mirdb-secondary': '#1e40af',
        'mirdb-dark': '#0f172a',
        'mirdb-light': '#f8fafc',
      }
    },
  },
  plugins: [],
}
