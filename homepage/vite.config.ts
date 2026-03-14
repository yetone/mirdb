import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 3000,
  },
  build: {
    // Enable minification for production builds (default: 'esbuild')
    minify: 'esbuild',
    // Tree-shaking is enabled by default with Rollup
    rollupOptions: {
      output: {
        // Chunk splitting for better caching
        manualChunks: {
          vendor: ['react', 'react-dom'],
        },
      },
    },
    // Target modern browsers for smaller bundles
    target: 'es2020',
    // Generate source maps for debugging (can be disabled in prod)
    sourcemap: false,
    // CSS code splitting
    cssCodeSplit: true,
  },
})
