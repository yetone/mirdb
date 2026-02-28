import { defineConfig } from 'vite';

export default defineConfig({
  root: 'src',
  build: {
    outDir: '../dist',
    emptyOutDir: true,
  },
  server: {
    port: 3000,
    open: true,
  },
  test: {
    environment: 'jsdom',
    include: ['../tests/unit/**/*.test.js', '../tests/integration/**/*.test.js'],
    globals: true,
  },
});
