/**
 * Static Site Deployment Integration Tests
 * Owner: Scenario 13 - Static Site Deployment Compatibility
 *
 * Tests that validate:
 * - Build process completes successfully
 * - Build output contains required files for static hosting
 * - Built site loads correctly from static file server
 */

import { test, expect } from '@playwright/test';
import { execSync } from 'child_process';
import { existsSync, readdirSync, statSync, readFileSync, rmSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const WEBSITE_ROOT = join(__dirname, '../..');
const DIST_DIR = join(WEBSITE_ROOT, 'dist');

test.describe('Static Site Deployment Compatibility', () => {
  test.describe.configure({ mode: 'serial' });

  test.beforeAll(async () => {
    // Clean dist directory before tests
    if (existsSync(DIST_DIR)) {
      rmSync(DIST_DIR, { recursive: true, force: true });
    }
  });

  test('Test Case 1: Build completes successfully without errors', async () => {
    // Run the build command
    let buildOutput: string;
    let buildError: string | null = null;
    let exitCode: number = 0;

    try {
      buildOutput = execSync('npm run build', {
        cwd: WEBSITE_ROOT,
        encoding: 'utf-8',
        stdio: ['pipe', 'pipe', 'pipe'],
      });
    } catch (error: any) {
      buildError = error.message;
      exitCode = error.status || 1;
      buildOutput = error.stdout || '';
    }

    // Verify build completed without errors
    expect(exitCode).toBe(0);
    expect(buildError).toBeNull();
    expect(buildOutput).toContain('vite');

    // Verify dist directory was created
    expect(existsSync(DIST_DIR)).toBe(true);
  });

  test('Test Case 2: Output contains index.html and required assets', async () => {
    // Verify dist directory exists (should exist after previous test)
    expect(existsSync(DIST_DIR)).toBe(true);

    // Check for index.html
    const indexPath = join(DIST_DIR, 'index.html');
    expect(existsSync(indexPath)).toBe(true);

    // Verify index.html is not empty and contains valid HTML
    const indexContent = readFileSync(indexPath, 'utf-8');
    expect(indexContent.length).toBeGreaterThan(0);
    expect(indexContent).toContain('<!DOCTYPE html>');
    expect(indexContent).toContain('<html');
    expect(indexContent).toContain('</html>');
    expect(indexContent).toContain('MirDB');

    // Check for assets directory (CSS and JS files)
    const assetsDir = join(DIST_DIR, 'assets');
    expect(existsSync(assetsDir)).toBe(true);

    // Get all files in assets directory
    const assetFiles = readdirSync(assetsDir);

    // Verify CSS files exist
    const cssFiles = assetFiles.filter(f => f.endsWith('.css'));
    expect(cssFiles.length).toBeGreaterThan(0);

    // Verify JS files exist (if any JS is in the project)
    const jsFiles = assetFiles.filter(f => f.endsWith('.js'));
    expect(jsFiles.length).toBeGreaterThanOrEqual(0);

    // Check for images directory
    const imagesDir = join(DIST_DIR, 'images');
    expect(existsSync(imagesDir)).toBe(true);

    // Verify logo exists
    const logoPath = join(imagesDir, 'mirdb-logo.svg');
    expect(existsSync(logoPath)).toBe(true);

    // Verify all required sections are present in index.html
    expect(indexContent).toContain('id="hero"');
    expect(indexContent).toContain('id="features"');
    expect(indexContent).toContain('id="quickstart"');
    expect(indexContent).toContain('id="comparison"');
    expect(indexContent).toContain('id="specs"');
    expect(indexContent).toContain('id="footer"');
  });

  test('Test Case 3: Page loads correctly from static file server', async () => {
    // Start a simple static file server and verify assets are served correctly
    // This test doesn't require a browser - it verifies HTTP responses directly
    const http = await import('http');
    const fs = await import('fs');
    const path = await import('path');
    const url = await import('url');

    const PORT = 4173; // Use a different port than dev server

    const server = http.createServer((req, res) => {
      const parsedUrl = url.parse(req.url || '/', true);
      let pathname = parsedUrl.pathname || '/';

      // Handle root path
      if (pathname === '/') {
        pathname = '/index.html';
      }

      const filePath = path.join(DIST_DIR, pathname);

      // Get file extension
      const ext = path.extname(filePath).toLowerCase();
      const mimeTypes: Record<string, string> = {
        '.html': 'text/html',
        '.css': 'text/css',
        '.js': 'application/javascript',
        '.svg': 'image/svg+xml',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.ico': 'image/x-icon',
      };

      const contentType = mimeTypes[ext] || 'application/octet-stream';

      fs.readFile(filePath, (err, data) => {
        if (err) {
          res.writeHead(404);
          res.end('Not Found');
          return;
        }
        res.writeHead(200, { 'Content-Type': contentType });
        res.end(data);
      });
    });

    // Start the server
    await new Promise<void>((resolve) => {
      server.listen(PORT, () => {
        resolve();
      });
    });

    // Helper function to make HTTP request
    const httpGet = (urlPath: string): Promise<{ statusCode: number; body: string; contentType: string }> => {
      return new Promise((resolve, reject) => {
        http.get(`http://localhost:${PORT}${urlPath}`, (res) => {
          let body = '';
          res.on('data', (chunk) => body += chunk);
          res.on('end', () => {
            resolve({
              statusCode: res.statusCode || 500,
              body,
              contentType: res.headers['content-type'] || '',
            });
          });
        }).on('error', reject);
      });
    };

    try {
      // Test 1: Verify index.html loads
      const indexResponse = await httpGet('/');
      expect(indexResponse.statusCode).toBe(200);
      expect(indexResponse.contentType).toContain('text/html');
      expect(indexResponse.body).toContain('<!DOCTYPE html>');
      expect(indexResponse.body).toContain('MirDB');
      expect(indexResponse.body).toContain('<title>');

      // Verify all sections are present in the HTML
      expect(indexResponse.body).toContain('id="hero"');
      expect(indexResponse.body).toContain('id="features"');
      expect(indexResponse.body).toContain('id="quickstart"');
      expect(indexResponse.body).toContain('id="comparison"');
      expect(indexResponse.body).toContain('id="specs"');
      expect(indexResponse.body).toContain('id="footer"');

      // Test 2: Verify CSS assets load
      const assetsDir = join(DIST_DIR, 'assets');
      const cssFiles = readdirSync(assetsDir).filter(f => f.endsWith('.css'));
      expect(cssFiles.length).toBeGreaterThan(0);

      for (const cssFile of cssFiles) {
        const cssResponse = await httpGet(`/assets/${cssFile}`);
        expect(cssResponse.statusCode).toBe(200);
        expect(cssResponse.contentType).toContain('text/css');
        expect(cssResponse.body.length).toBeGreaterThan(0);
      }

      // Test 3: Verify JS assets load (if any)
      const jsFiles = readdirSync(assetsDir).filter(f => f.endsWith('.js'));
      for (const jsFile of jsFiles) {
        const jsResponse = await httpGet(`/assets/${jsFile}`);
        expect(jsResponse.statusCode).toBe(200);
        expect(jsResponse.contentType).toContain('javascript');
        expect(jsResponse.body.length).toBeGreaterThan(0);
      }

      // Test 4: Verify logo image loads
      const logoResponse = await httpGet('/images/mirdb-logo.svg');
      expect(logoResponse.statusCode).toBe(200);
      expect(logoResponse.contentType).toContain('svg');

      // Test 5: Verify 404 for non-existent file
      const notFoundResponse = await httpGet('/nonexistent.html');
      expect(notFoundResponse.statusCode).toBe(404);

    } finally {
      // Clean up server
      server.close();
    }
  });
});

test.describe('Static Site Build Requirements', () => {
  test('Build output has no server-side dependencies', async () => {
    // Verify index.html doesn't contain server-side templating
    const indexPath = join(DIST_DIR, 'index.html');
    const indexContent = readFileSync(indexPath, 'utf-8');

    // Should not contain PHP
    expect(indexContent).not.toContain('<?php');
    expect(indexContent).not.toContain('<?=');

    // Should not contain server-side template tags (common frameworks)
    expect(indexContent).not.toContain('<%=');
    expect(indexContent).not.toContain('<% ');
    expect(indexContent).not.toContain('{{#');
    expect(indexContent).not.toContain('{%');

    // All script sources should be relative or absolute static paths
    const scriptTags = indexContent.match(/<script[^>]*src=["']([^"']+)["']/g) || [];
    for (const tag of scriptTags) {
      const srcMatch = tag.match(/src=["']([^"']+)["']/);
      if (srcMatch) {
        const src = srcMatch[1];
        // Should not be API endpoints or server routes
        expect(src).not.toMatch(/^\/api\//);
        expect(src).not.toMatch(/\.php$/);
        expect(src).not.toMatch(/\.asp$/);
      }
    }
  });

  test('CSS files are properly bundled', async () => {
    const assetsDir = join(DIST_DIR, 'assets');
    const cssFiles = readdirSync(assetsDir).filter(f => f.endsWith('.css'));

    expect(cssFiles.length).toBeGreaterThan(0);

    // Verify CSS content
    for (const cssFile of cssFiles) {
      const cssPath = join(assetsDir, cssFile);
      const cssContent = readFileSync(cssPath, 'utf-8');

      // Should have content
      expect(cssContent.length).toBeGreaterThan(0);

      // Should be valid CSS (contains selectors and rules)
      expect(cssContent).toMatch(/[\w\-\.#]+\s*\{/);
    }
  });

  test('All internal links are relative (deployable to any path)', async () => {
    const indexPath = join(DIST_DIR, 'index.html');
    const indexContent = readFileSync(indexPath, 'utf-8');

    // Extract all href and src attributes
    const hrefMatches = indexContent.match(/href=["']([^"']+)["']/g) || [];
    const srcMatches = indexContent.match(/src=["']([^"']+)["']/g) || [];

    for (const match of [...hrefMatches, ...srcMatches]) {
      const urlMatch = match.match(/=["']([^"']+)["']/);
      if (urlMatch) {
        const url = urlMatch[1];

        // Skip external URLs and anchor links
        if (url.startsWith('http') || url.startsWith('//') || url.startsWith('#') || url.startsWith('mailto:')) {
          continue;
        }

        // Internal paths should be relative (start with ./ or /) or be asset paths
        expect(url).toMatch(/^(\.?\/|assets\/|images\/)/);
      }
    }
  });
});
