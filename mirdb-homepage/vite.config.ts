/// <reference types="vitest" />
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';
import viteCompression from 'vite-plugin-compression';

export default defineConfig({
  plugins: [
    react(),
    // Gzip compression for production assets
    viteCompression({
      algorithm: 'gzip',
      ext: '.gz',
      threshold: 1024, // Only compress files larger than 1KB
      deleteOriginFile: false,
    }),
    // Brotli compression for production assets (better compression ratio)
    viteCompression({
      algorithm: 'brotliCompress',
      ext: '.br',
      threshold: 1024,
      deleteOriginFile: false,
    }),
  ],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  build: {
    outDir: 'dist',
    // Enable minification with esbuild (fastest, good compression)
    minify: 'esbuild',
    sourcemap: false,
    // Target modern browsers for smaller bundle size
    target: 'es2020',
    // CSS optimization
    cssMinify: true,
    // Chunk splitting for better caching
    rollupOptions: {
      output: {
        // Separate vendor chunks for better caching
        manualChunks: {
          vendor: ['react', 'react-dom'],
        },
        // Use content hash for cache busting
        entryFileNames: 'assets/[name].[hash].js',
        chunkFileNames: 'assets/[name].[hash].js',
        assetFileNames: 'assets/[name].[hash].[ext]',
      },
    },
    // Enable tree-shaking for smaller bundles
    treeshake: true,
    // Set chunk size warning limit
    chunkSizeWarningLimit: 500,
  },
  server: {
    port: 5173,
  },
  // Preview server configuration for testing production builds
  preview: {
    port: 4173,
    headers: {
      // Enable compression headers for preview server testing
      'Content-Encoding': 'gzip',
    },
  },
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: './src/setupTests.ts',
    include: ['tests/unit/**/*.spec.ts', 'tests/unit/**/*.spec.tsx', 'tests/integration/**/*.spec.ts'],
    exclude: ['tests/e2e/**/*', 'node_modules'],
  },
});
