/**
 * Vitest Configuration for Unit Tests
 *
 * Configuration for running DOM-based unit tests using JSDOM
 */

import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    environment: 'jsdom',
    include: ['tests/unit/**/*.test.ts'],
    root: path.resolve(__dirname, '..'),
    globals: true,
  },
});
