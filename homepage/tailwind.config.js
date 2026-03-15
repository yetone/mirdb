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
          50: '#fef3f2',
          100: '#fee4e2',
          200: '#fecdca',
          300: '#fca5a5',
          400: '#f87171',
          500: '#ef4444',
          600: '#dc2626',
          700: '#b91c1c',
          800: '#991b1b',
          900: '#7f1d1d',
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
