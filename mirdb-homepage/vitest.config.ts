import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: './tests/setup.tsx',
    exclude: [
      '**/node_modules/**',
      '**/dist/**',
      '**/tests/e2e/**',
    ],
  },
})
