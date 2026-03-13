import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
// Performance optimizations for NFR-1 (load time < 2s)
// Owner: Scenario 11 - Performance Requirements
export default defineConfig({
  plugins: [react()],
  build: {
    // Enable minification for smaller bundles
    minify: 'esbuild',
    // Target modern browsers for smaller output
    target: 'esnext',
    // Generate source maps only in development
    sourcemap: false,
    // Optimize chunk size
    chunkSizeWarningLimit: 200, // Warn if chunks exceed 200KB
    rollupOptions: {
      output: {
        // Optimize asset file names for caching
        chunkFileNames: 'assets/js/[name]-[hash].js',
        entryFileNames: 'assets/js/[name]-[hash].js',
        assetFileNames: 'assets/[name]-[hash][extname]',
      },
    },
    // CSS code splitting
    cssCodeSplit: true,
    // Minimize CSS
    cssMinify: true,
  },
  // Optimize dependencies
  optimizeDeps: {
    // Include React dependencies for faster dev startup
    include: ['react', 'react-dom'],
  },
})
