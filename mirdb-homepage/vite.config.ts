import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  build: {
    // Disable source maps in production for smaller bundle size and security
    sourcemap: false,
    // Enable minification for production
    minify: 'esbuild',
    // Target modern browsers for smaller bundle
    target: 'es2020',
    // Configure chunk splitting for optimal caching
    rollupOptions: {
      output: {
        // Optimize chunk names
        chunkFileNames: 'assets/[name]-[hash].js',
        entryFileNames: 'assets/[name]-[hash].js',
        assetFileNames: 'assets/[name]-[hash].[ext]',
        // Manual chunk splitting for vendor code
        manualChunks: {
          vendor: ['react', 'react-dom'],
        },
      },
    },
    // Emit compressed size info during build
    reportCompressedSize: true,
    // CSS code splitting
    cssCodeSplit: true,
    // Asset inlining threshold (4KB)
    assetsInlineLimit: 4096,
  },
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: './tests/setup.tsx',
  },
})
