/**
 * Vitest Configuration
 *
 * This file is created by the first scenario builder.
 *
 * Configuration:
 * - Test environment (jsdom for React)
 * - Coverage settings
 * - Setup files path
 * - Test file patterns
 */
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: ['./__tests__/setup.ts'],
    include: ['__tests__/**/*.{test,spec}.{ts,tsx}'],
    coverage: {
      reporter: ['text', 'json', 'html'],
      exclude: ['node_modules/', '__tests__/setup.ts'],
    },
  },
})
