import { defineConfig } from 'astro/config';

export default defineConfig({
  site: 'https://mirdb.dev',
  srcDir: 'src',
  publicDir: 'public',
  outDir: 'dist',
  integrations: [],
  vite: {
    test: {
      environment: 'happy-dom',
      globals: true,
      include: ['tests/**/*.test.ts'],
    },
  },
});
