// @ts-check
import { defineConfig } from 'astro/config';

import tailwindcss from '@tailwindcss/vite';

// https://astro.build/config
export default defineConfig({
  // Optimize for performance
  build: {
    inlineStylesheets: 'auto',
    minify: true,
    assets: 'assets'
  },
  vite: {
    plugins: [tailwindcss()],
    build: {
      rollupOptions: {
        output: {
          manualChunks: {
            // Split vendor code for better caching
            // Currently minimal since we're mostly static
          }
        }
      }
    }
  }
});