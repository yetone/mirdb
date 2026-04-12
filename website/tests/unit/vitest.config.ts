/**
 * Vitest Configuration
 * Owner: First builder (shared resource)
 */

import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'jsdom',
    globals: true,
    include: ['**/*.test.ts', '**/*.test.js'],
  },
});
