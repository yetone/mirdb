import { defineConfig } from 'astro/config';

export default defineConfig({
  site: 'https://mirdb.io',
  output: 'static',
  build: {
    assets: '_assets',
    format: 'file'
  },
  vite: {
    build: {
      cssMinify: true,
      minify: true,
      rollupOptions: {
        output: {
          assetFileNames: '_assets/[name].[hash][extname]',
          chunkFileNames: '_assets/[name].[hash].js',
          entryFileNames: '_assets/[name].[hash].js'
        }
      }
    }
  }
});
