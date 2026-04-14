/**
 * Vite Configuration
 * Owner: First Builder
 *
 * Configures:
 * - Root directory (src)
 * - Build output (dist)
 * - Dev server settings
 */
import { defineConfig } from 'vite';

export default defineConfig({
  root: 'src',
  build: {
    outDir: '../dist',
    emptyOutDir: true,
  },
  server: {
    port: 5173,
  },
  preview: {
    port: 4173,
  },
});
