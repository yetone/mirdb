/**
 * Vitest Unit Test Configuration
 * Owner: Scenario 11 - Performance and Static Site Build
 *
 * Expected contents:
 * - Test environment: jsdom
 * - Test directory: tests/unit
 * - Coverage reporting configuration
 * - Module mocking for clipboard API
 */

import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'jsdom',
    include: ['tests/unit/**/*.test.js'],
  },
});
