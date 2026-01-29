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
        // Using CSS variables to support theme switching
        background: 'var(--color-background)',
        surface: 'var(--color-surface)',
        border: 'var(--color-border)',
        'text-primary': 'var(--color-text-primary)',
        'text-secondary': 'var(--color-text-secondary)',
        accent: 'var(--color-accent)',
        success: 'var(--color-success)',
        warning: 'var(--color-warning)',
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'SF Mono', 'Consolas', 'monospace'],
        sans: ['system-ui', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'sans-serif'],
      },
    },
  },
  plugins: [],
};
