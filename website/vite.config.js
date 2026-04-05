/**
 * Vite Build Configuration
 * Owner: Scenario 13 - Static Site Deployment
 * Basic setup created by first builder
 */
import { defineConfig } from 'vite';

export default defineConfig({
  root: 'src',
  publicDir: '../public',
  build: {
    outDir: '../dist',
    emptyOutDir: true
  },
  server: {
    port: 5173,
    open: false
  }
});
