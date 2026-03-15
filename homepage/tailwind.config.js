/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        primary: {
          50: '#eff6ff',
          100: '#dbeafe',
          200: '#bfdbfe',
          300: '#93c5fd',
          400: '#60a5fa',
          500: '#3b82f6',
          600: '#2563eb',
          700: '#1d4ed8',
          800: '#1e40af',
          900: '#1e3a8a',
        },
        rust: {
          50: '#fdf4f3',
          100: '#fce7e4',
          200: '#fad2cd',
          300: '#f5b1a8',
          400: '#ec8474',
          500: '#de5c48',
          600: '#cb422f',
          700: '#aa3424',
          800: '#8c2f22',
          900: '#752c22',
        },
      },
    },
  },
  plugins: [],
}
