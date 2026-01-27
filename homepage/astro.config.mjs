import { defineConfig } from 'astro/config';

export default defineConfig({
  site: 'https://mirdb.io',
  build: {
    assets: '_assets'
  },
  vite: {
    build: {
      cssMinify: true,
      minify: true
    }
  }
});
