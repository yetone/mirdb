/**
 * Vite Build Configuration
 * Owner: Scenario 13 - Static Site Deployment
 *
 * Configuration optimized for static site hosting:
 * - GitHub Pages, Netlify, Vercel, Cloudflare Pages, etc.
 * - No server-side dependencies required
 * - Assets are properly bundled and hashed for cache busting
 */
import { defineConfig } from 'vite';

export default defineConfig({
  root: 'src',
  publicDir: '../public',
  base: './', // Use relative paths for static hosting compatibility
  build: {
    outDir: '../dist',
    emptyOutDir: true,
    // Ensure assets are properly handled for static hosting
    assetsDir: 'assets',
    // Generate source maps for debugging (can be disabled in production)
    sourcemap: false,
    // Optimize for static deployment
    rollupOptions: {
      output: {
        // Use consistent chunk naming for cache management
        chunkFileNames: 'assets/[name]-[hash].js',
        entryFileNames: 'assets/[name]-[hash].js',
        assetFileNames: 'assets/[name]-[hash].[ext]',
      },
    },
    // Minify for production
    minify: 'esbuild',
    // Target modern browsers for smaller bundles
    target: 'es2020',
  },
  server: {
    port: 5173,
    open: false,
  },
  preview: {
    port: 4173,
    open: false,
  },
});
