/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  // Dark mode is enabled by default via 'class' strategy
  // The app uses dark theme as the default (NFR-3: dark mode preference)
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        primary: {
          50: '#f0f9ff',
          100: '#e0f2fe',
          200: '#bae6fd',
          300: '#7dd3fc',
          400: '#38bdf8',
          500: '#0ea5e9',
          600: '#0284c7',
          700: '#0369a1',
          800: '#075985',
          900: '#0c4a6e',
          950: '#082f49',
        },
        // Dark theme color definitions for WCAG AA compliance
        // Background colors provide low luminance (<50%)
        // Text colors provide high contrast (4.5:1 minimum)
        dark: {
          // Primary background: slate-900 (#0f172a) - luminance ~3%
          bg: '#0f172a',
          // Secondary background for cards: slate-800 (#1e293b) - luminance ~7%
          surface: '#1e293b',
          // Code block background: slate-950 (#020617) - luminance ~1%
          code: '#020617',
          // Border color for subtle separation
          border: '#334155',
        },
      },
    },
  },
  plugins: [],
}
