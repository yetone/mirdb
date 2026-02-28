import { defineConfig } from 'vite';
import { readFileSync } from 'fs';
import { resolve } from 'path';

export default defineConfig({
  root: 'src',
  build: {
    outDir: '../dist',
    emptyOutDir: true,
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'src/index.html'),
        '404': resolve(__dirname, 'src/404.html'),
      },
    },
  },
  server: {
    port: 3000,
    open: true,
  },
  plugins: [
    {
      name: 'spa-fallback',
      configureServer(server) {
        server.middlewares.use((req, res, next) => {
          // Skip existing files and dev server endpoints
          if (
            req.url === '/' ||
            req.url.startsWith('/@') ||
            req.url.startsWith('/node_modules') ||
            req.url.includes('.')
          ) {
            return next();
          }
          // Serve 404.html for non-existent routes
          try {
            const html = readFileSync(resolve(__dirname, 'src/404.html'), 'utf-8');
            res.statusCode = 404;
            res.setHeader('Content-Type', 'text/html');
            res.end(html);
          } catch {
            next();
          }
        });
      },
    },
  ],
  test: {
    environment: 'jsdom',
    include: ['../tests/unit/**/*.test.js', '../tests/integration/**/*.test.js'],
    globals: true,
  },
});
