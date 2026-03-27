/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{html,js}",
    "./docs/**/*.{html,js}"
  ],
  theme: {
    extend: {
      colors: {
        'mirdb-primary': '#4F46E5',
        'mirdb-secondary': '#6366F1',
        'mirdb-accent': '#818CF8'
      },
      fontFamily: {
        'mono': ['Fira Code', 'Monaco', 'Consolas', 'monospace']
      }
    },
  },
  plugins: [],
}
