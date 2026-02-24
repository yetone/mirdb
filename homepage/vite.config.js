import { defineConfig } from 'vite';

export default defineConfig({
  root: '.',
  build: {
    outDir: 'dist',
    assetsDir: 'assets',
  },
  server: {
    port: 3000,
    open: true,
  },
  test: {
    environment: 'jsdom',
    include: ['tests/unit/**/*.test.js'],
    exclude: ['tests/integration/**/*.test.js', 'tests/e2e/**/*.test.js'],
    globals: true,
  },
});
