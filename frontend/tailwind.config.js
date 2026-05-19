/** @type {import('tailwindcss').Config} */
export default {
  content: [
    './index.html',
    './src/**/*.{js,ts,jsx,tsx}',
  ],
  theme: {
    extend: {
      screens: {
        'mobile': '375px',
        'tablet': '768px',
        'desktop': '1024px',
        'wide': '1440px',
      },
    },
  },
  plugins: [],
}
