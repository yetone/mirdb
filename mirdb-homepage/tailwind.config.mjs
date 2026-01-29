/**
 * Tailwind CSS Configuration.
 * Owner: First builder
 *
 * Custom configuration:
 * - Dark-first color palette from PRD
 * - Background: #0d1117
 * - Surface: #161b22
 * - Border: #30363d
 * - Text Primary: #c9d1d9
 * - Text Secondary: #8b949e
 * - Accent: #58a6ff
 * - Success: #3fb950
 * - Warning: #d29922
 * - Custom fonts (JetBrains Mono for headings)
 */
/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        background: '#0d1117',
        surface: '#161b22',
        border: '#30363d',
        'text-primary': '#c9d1d9',
        'text-secondary': '#8b949e',
        accent: '#58a6ff',
        success: '#3fb950',
        warning: '#d29922',
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'SF Mono', 'Consolas', 'monospace'],
        sans: ['system-ui', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'sans-serif'],
      },
    },
  },
  plugins: [],
};
