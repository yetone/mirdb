/**
 * Vitest Configuration
 *
 * Configures unit test settings separately from Vite build config.
 * This allows tests to run from the project root while the build uses src/ as root.
 */
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Run tests from project root, not src/
    root: '.',
    // Include unit tests
    include: ['tests/unit/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}'],
    // Exclude build outputs and node_modules
    exclude: ['node_modules', 'dist', 'src'],
    // Enable globals for describe, it, expect
    globals: true,
  },
});
