import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { resolve } from 'path'
import { writeFileSync, readFileSync } from 'fs'

// Plugin to create 404.html for SPA routing on GitHub Pages
const create404Plugin = () => ({
  name: 'create-404',
  closeBundle() {
    const distPath = resolve(__dirname, 'dist')
    const indexPath = resolve(distPath, 'index.html')
    const notFoundPath = resolve(distPath, '404.html')
    try {
      const indexHtml = readFileSync(indexPath, 'utf-8')
      writeFileSync(notFoundPath, indexHtml)
    } catch {
      // Ignore if index.html doesn't exist yet
    }
  },
})

// https://vitejs.dev/config/
// NFR-4: Static deployment compatibility for GitHub Pages
export default defineConfig({
  plugins: [react(), create404Plugin()],
  // Use relative paths for subdirectory hosting (e.g., username.github.io/repo/)
  base: './',
  build: {
    outDir: 'dist',
    assetsDir: 'assets',
    // Generate source maps for debugging
    sourcemap: false,
    // Ensure clean builds
    emptyOutDir: true,
    // Optimize for static hosting
    rollupOptions: {
      output: {
        // Ensure consistent asset file naming
        entryFileNames: 'assets/[name]-[hash].js',
        chunkFileNames: 'assets/[name]-[hash].js',
        assetFileNames: 'assets/[name]-[hash][extname]',
      },
    },
  },
})
