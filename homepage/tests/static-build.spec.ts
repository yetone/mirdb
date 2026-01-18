import { test, expect } from '@playwright/test';
import { execSync } from 'child_process';
import { existsSync, statSync, readdirSync } from 'fs';
import { join } from 'path';

const DIST_DIR = join(process.cwd(), 'dist');

test.describe('Static Site Build', () => {
  test.describe.configure({ mode: 'serial' });

  test('build command completes successfully with exit code 0', async () => {
    // Run the build command and capture the result
    const result = execSync('npm run build', {
      encoding: 'utf-8',
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    // Verify build output contains success indicators
    expect(result).toContain('Complete!');
    expect(result).toContain('generating static routes');
  });

  test('dist/index.html exists after build', async () => {
    const indexPath = join(DIST_DIR, 'index.html');
    const exists = existsSync(indexPath);
    expect(exists).toBe(true);

    // Verify index.html is non-empty
    const stats = statSync(indexPath);
    expect(stats.size).toBeGreaterThan(0);
  });

  test('compiled CSS files are present and non-empty in dist', async () => {
    const astroDir = join(DIST_DIR, '_astro');
    expect(existsSync(astroDir)).toBe(true);

    const files = readdirSync(astroDir);
    const cssFiles = files.filter(f => f.endsWith('.css'));

    // At least one CSS file should exist
    expect(cssFiles.length).toBeGreaterThan(0);

    // Verify each CSS file is non-empty
    for (const cssFile of cssFiles) {
      const cssPath = join(astroDir, cssFile);
      const stats = statSync(cssPath);
      expect(stats.size).toBeGreaterThan(0);
    }
  });

  test('JavaScript bundle is minimal or zero (Astro default)', async () => {
    const astroDir = join(DIST_DIR, '_astro');
    const files = readdirSync(astroDir);
    const jsFiles = files.filter(f => f.endsWith('.js'));

    // Astro's default is zero JS for static sites
    // If there are JS files, they should be minimal
    if (jsFiles.length > 0) {
      let totalJsSize = 0;
      for (const jsFile of jsFiles) {
        const jsPath = join(astroDir, jsFile);
        const stats = statSync(jsPath);
        totalJsSize += stats.size;
      }
      // Total JS should be less than 50KB for a simple static site
      expect(totalJsSize).toBeLessThan(50000);
    }
    // Zero JS files is the expected default - test passes
  });

  test('site works correctly when served as static files', async () => {
    // Start a simple static server for the dist folder
    const { createServer } = await import('http');
    const { readFileSync, existsSync: fsExistsSync } = await import('fs');
    const { join: pathJoin, extname } = await import('path');
    const http = await import('http');

    const mimeTypes: Record<string, string> = {
      '.html': 'text/html',
      '.css': 'text/css',
      '.js': 'application/javascript',
      '.json': 'application/json',
      '.png': 'image/png',
      '.jpg': 'image/jpeg',
      '.gif': 'image/gif',
      '.svg': 'image/svg+xml',
      '.webp': 'image/webp',
    };

    const server = createServer((req, res) => {
      let filePath = pathJoin(DIST_DIR, req.url === '/' ? 'index.html' : req.url || '');

      if (fsExistsSync(filePath)) {
        const ext = extname(filePath);
        const contentType = mimeTypes[ext] || 'application/octet-stream';
        const content = readFileSync(filePath);
        res.writeHead(200, { 'Content-Type': contentType });
        res.end(content);
      } else {
        res.writeHead(404);
        res.end('Not found');
      }
    });

    const PORT = 4322;
    await new Promise<void>((resolve) => {
      server.listen(PORT, () => resolve());
    });

    try {
      // Test using HTTP request instead of browser
      const response = await new Promise<{ statusCode: number | undefined; body: string }>((resolve, reject) => {
        const req = http.request(
          { hostname: 'localhost', port: PORT, path: '/', method: 'GET' },
          (res) => {
            let body = '';
            res.on('data', (chunk) => (body += chunk));
            res.on('end', () => resolve({ statusCode: res.statusCode, body }));
          }
        );
        req.on('error', reject);
        req.end();
      });

      // Verify response status
      expect(response.statusCode).toBe(200);

      // Verify HTML content contains expected elements
      expect(response.body).toContain('<!DOCTYPE html>');
      expect(response.body).toContain('<html');
      expect(response.body).toContain('</html>');

      // Verify CSS link is present
      expect(response.body).toMatch(/<link[^>]+\.css[^>]*>/);

      // Verify key content from the MirDB homepage
      expect(response.body).toContain('MirDB');
    } finally {
      server.close();
    }
  });
});
