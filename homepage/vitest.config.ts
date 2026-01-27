import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/unit/**/*.test.ts', 'tests/integration/**/*.spec.ts'],
    environment: 'node',
    globals: true,
    testTimeout: 60000
  }
});
