import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: './tests/setup/test-utils.tsx',
    css: true,
    exclude: ['**/node_modules/**', '**/tests/e2e/**'],
  },
})
