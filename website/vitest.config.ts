/**
 * Vitest Configuration
 * Owner: First Builder
 *
 * Configures:
 * - Test directory (tests/unit)
 * - Coverage settings
 * - TypeScript support
 * - DOM environment for unit tests
 */
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/unit/**/*.test.ts'],
    environment: 'jsdom',
    globals: true,
  },
});
