/**
 * Build Process Integration Tests
 * Owner: Scenario 14 - Static Site Build Process
 *
 * Validates the static site generation builds successfully with no backend dependencies:
 * - Build completes without errors
 * - Build time < 30 seconds
 * - Output contains expected files
 * - Site can be served statically
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as http from 'http';
import { chromium, Browser, Page, ConsoleMessage } from 'playwright';

const ROOT_DIR = path.resolve(__dirname, '../../');
const DIST_DIR = path.join(ROOT_DIR, 'dist');

describe('Static Site Build Process', () => {
  // Clean build before tests
  beforeAll(() => {
    // Clean dist directory if it exists
    if (fs.existsSync(DIST_DIR)) {
      fs.rmSync(DIST_DIR, { recursive: true, force: true });
    }
  });

  describe('Build Execution', () => {
    it('should complete build successfully without errors', () => {
      // Test Case 1: Build completes successfully without errors
      let buildError: Error | null = null;
      let buildOutput = '';

      try {
        buildOutput = execSync('npm run build', {
          cwd: ROOT_DIR,
          encoding: 'utf8',
          timeout: 60000,
          stdio: ['pipe', 'pipe', 'pipe'],
        });
      } catch (error) {
        buildError = error as Error;
      }

      expect(buildError).toBeNull();
      expect(buildOutput).toContain('built in');
      expect(fs.existsSync(DIST_DIR)).toBe(true);
    });

    it('should complete build within 30 seconds', () => {
      // Test Case 2: Build completes within 30 seconds
      // Clean for fresh timing
      if (fs.existsSync(DIST_DIR)) {
        fs.rmSync(DIST_DIR, { recursive: true, force: true });
      }

      const startTime = Date.now();

      execSync('npm run build', {
        cwd: ROOT_DIR,
        encoding: 'utf8',
        timeout: 30000, // Timeout at 30 seconds
        stdio: ['pipe', 'pipe', 'pipe'],
      });

      const buildTime = Date.now() - startTime;

      expect(buildTime).toBeLessThan(30000);
      console.log(`Build completed in ${buildTime}ms`);
    });
  });

  describe('Build Output Verification', () => {
    it('should generate index.html in output directory', () => {
      // Test Case 3: Verify output directory contains index.html
      const indexPath = path.join(DIST_DIR, 'index.html');

      expect(fs.existsSync(indexPath)).toBe(true);

      const indexContent = fs.readFileSync(indexPath, 'utf8');
      expect(indexContent).toContain('<!DOCTYPE html>');
      expect(indexContent).toContain('<html');
      expect(indexContent).toContain('</html>');
    });

    it('should generate CSS assets', () => {
      const assetsDir = path.join(DIST_DIR, 'assets');
      expect(fs.existsSync(assetsDir)).toBe(true);

      const files = fs.readdirSync(assetsDir);
      const cssFiles = files.filter((f) => f.endsWith('.css'));

      expect(cssFiles.length).toBeGreaterThan(0);

      // Verify CSS file is non-empty and valid
      const cssPath = path.join(assetsDir, cssFiles[0]);
      const cssContent = fs.readFileSync(cssPath, 'utf8');
      expect(cssContent.length).toBeGreaterThan(0);
    });

    it('should generate JavaScript assets', () => {
      const assetsDir = path.join(DIST_DIR, 'assets');
      const files = fs.readdirSync(assetsDir);
      const jsFiles = files.filter((f) => f.endsWith('.js') && !f.endsWith('.gz') && !f.endsWith('.br'));

      expect(jsFiles.length).toBeGreaterThan(0);

      // Verify JS file is non-empty
      const jsPath = path.join(assetsDir, jsFiles[0]);
      const jsContent = fs.readFileSync(jsPath, 'utf8');
      expect(jsContent.length).toBeGreaterThan(0);
    });

    it('should generate compressed assets for production', () => {
      const assetsDir = path.join(DIST_DIR, 'assets');
      const files = fs.readdirSync(assetsDir);

      // Check for gzip compressed files
      const gzipFiles = files.filter((f) => f.endsWith('.gz'));
      expect(gzipFiles.length).toBeGreaterThan(0);

      // Check for brotli compressed files
      const brotliFiles = files.filter((f) => f.endsWith('.br'));
      expect(brotliFiles.length).toBeGreaterThan(0);
    });

    it('should generate valid HTML with required structure', () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      // Check for essential HTML structure
      expect(content).toContain('lang="en"');
      expect(content).toContain('<head>');
      expect(content).toContain('</head>');
      expect(content).toContain('<body>');
      expect(content).toContain('</body>');
      expect(content).toContain('<div id="root">');

      // Check for viewport meta tag (mobile responsive)
      expect(content).toContain('viewport');

      // Check for linked assets
      expect(content).toMatch(/\.js/);
      expect(content).toMatch(/\.css/);
    });
  });

  describe('Static File Server Compatibility', () => {
    let server: ReturnType<typeof http.createServer> | null = null;
    const TEST_PORT = 9876;

    afterAll(() => {
      if (server) {
        server.close();
      }
    });

    it('should serve built files from static file server', async () => {
      // Test Case 4: Site works when served from static file server
      // Create a simple static file server
      server = http.createServer((req, res) => {
        let filePath = path.join(DIST_DIR, req.url === '/' ? 'index.html' : req.url || '');

        // Remove query strings if present
        filePath = filePath.split('?')[0];

        const extname = path.extname(filePath);
        const contentTypeMap: Record<string, string> = {
          '.html': 'text/html',
          '.js': 'application/javascript',
          '.css': 'text/css',
        };

        const contentType = contentTypeMap[extname] || 'application/octet-stream';

        fs.readFile(filePath, (err, content) => {
          if (err) {
            res.writeHead(404);
            res.end('Not Found');
          } else {
            res.writeHead(200, { 'Content-Type': contentType });
            res.end(content);
          }
        });
      });

      await new Promise<void>((resolve) => {
        server!.listen(TEST_PORT, () => resolve());
      });

      // Make HTTP request to verify server responds
      const response = await new Promise<{ statusCode: number; body: string }>((resolve, reject) => {
        http.get(`http://localhost:${TEST_PORT}/`, (res) => {
          let body = '';
          res.on('data', (chunk) => (body += chunk));
          res.on('end', () =>
            resolve({
              statusCode: res.statusCode || 0,
              body,
            })
          );
        }).on('error', reject);
      });

      expect(response.statusCode).toBe(200);
      expect(response.body).toContain('<!DOCTYPE html>');
      expect(response.body).toContain('<html');
    });

    it('should serve CSS and JS assets correctly', async () => {
      // Check that CSS assets are served
      const assetsDir = path.join(DIST_DIR, 'assets');
      const files = fs.readdirSync(assetsDir);

      const cssFile = files.find((f) => f.endsWith('.css') && !f.endsWith('.gz') && !f.endsWith('.br'));
      const jsFile = files.find((f) => f.endsWith('.js') && !f.endsWith('.gz') && !f.endsWith('.br'));

      if (cssFile) {
        const cssResponse = await new Promise<{ statusCode: number }>((resolve, reject) => {
          http
            .get(`http://localhost:${TEST_PORT}/assets/${cssFile}`, (res) => {
              res.resume(); // Drain the response
              res.on('end', () => resolve({ statusCode: res.statusCode || 0 }));
            })
            .on('error', reject);
        });

        expect(cssResponse.statusCode).toBe(200);
      }

      if (jsFile) {
        const jsResponse = await new Promise<{ statusCode: number }>((resolve, reject) => {
          http
            .get(`http://localhost:${TEST_PORT}/assets/${jsFile}`, (res) => {
              res.resume();
              res.on('end', () => resolve({ statusCode: res.statusCode || 0 }));
            })
            .on('error', reject);
        });

        expect(jsResponse.statusCode).toBe(200);
      }
    });
  });

  describe('No Backend Dependencies', () => {
    it('should have no server-side rendering or API dependencies', () => {
      // Verify package.json doesn't have backend dependencies
      const packageJsonPath = path.join(ROOT_DIR, 'package.json');
      const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));

      // Check for common backend dependencies that shouldn't be present
      const backendDeps = ['express', 'fastify', 'koa', 'hapi', 'next', 'nuxt', 'nest'];
      const allDeps = {
        ...(packageJson.dependencies || {}),
        ...(packageJson.devDependencies || {}),
      };

      const foundBackendDeps = backendDeps.filter((dep) => dep in allDeps);

      // Allow 'next' only if it's configured for static export (which it isn't in this project)
      expect(foundBackendDeps.filter((d) => d !== 'next')).toHaveLength(0);
    });

    it('should not contain server-side code markers in build output', () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      // Check that the HTML doesn't have server-side rendering markers
      expect(content).not.toContain('__NEXT_DATA__');
      expect(content).not.toContain('__NUXT__');
      expect(content).not.toContain('window.__PRELOADED_STATE__');

      // Verify it has the client-side React root
      expect(content).toContain('id="root"');
    });

    it('should produce pure static files with no runtime dependencies', () => {
      // All files in dist should be static assets
      const getAllFiles = (dir: string): string[] => {
        const files: string[] = [];
        const items = fs.readdirSync(dir, { withFileTypes: true });

        for (const item of items) {
          const fullPath = path.join(dir, item.name);
          if (item.isDirectory()) {
            files.push(...getAllFiles(fullPath));
          } else {
            files.push(fullPath);
          }
        }
        return files;
      };

      const allFiles = getAllFiles(DIST_DIR);
      const allowedExtensions = ['.html', '.css', '.js', '.gz', '.br', '.ico', '.svg', '.png', '.jpg', '.jpeg', '.webp', '.json', '.txt', '.xml'];

      for (const file of allFiles) {
        const ext = path.extname(file);
        expect(allowedExtensions).toContain(ext);
      }
    });
  });

  describe('Console Error E2E Check', () => {
    let browser: Browser | null = null;
    let staticServer: ReturnType<typeof http.createServer> | null = null;
    const E2E_PORT = 9877;

    beforeAll(async () => {
      // Create static file server for E2E tests
      staticServer = http.createServer((req, res) => {
        let filePath = path.join(DIST_DIR, req.url === '/' ? 'index.html' : req.url || '');
        filePath = filePath.split('?')[0];

        const extname = path.extname(filePath);
        const contentTypeMap: Record<string, string> = {
          '.html': 'text/html',
          '.js': 'application/javascript',
          '.css': 'text/css',
          '.svg': 'image/svg+xml',
          '.png': 'image/png',
          '.ico': 'image/x-icon',
        };

        const contentType = contentTypeMap[extname] || 'application/octet-stream';

        fs.readFile(filePath, (err, content) => {
          if (err) {
            res.writeHead(404);
            res.end('Not Found');
          } else {
            res.writeHead(200, { 'Content-Type': contentType });
            res.end(content);
          }
        });
      });

      await new Promise<void>((resolve) => {
        staticServer!.listen(E2E_PORT, () => resolve());
      });

      // Launch browser
      browser = await chromium.launch({ headless: true });
    }, 60000);

    afterAll(async () => {
      if (browser) {
        await browser.close();
      }
      if (staticServer) {
        staticServer.close();
      }
    });

    it('should have no JavaScript console errors on page load', async () => {
      // Test Case 5: No console errors on page load
      if (!browser) {
        throw new Error('Browser not initialized');
      }

      const page: Page = await browser.newPage();
      const consoleErrors: string[] = [];

      // Collect console errors (excluding favicon.ico 404 which is expected)
      page.on('console', (msg: ConsoleMessage) => {
        if (msg.type() === 'error') {
          const text = msg.text();
          // Ignore favicon 404 errors as they're not JavaScript errors
          if (!text.includes('favicon.ico')) {
            consoleErrors.push(text);
          }
        }
      });

      // Collect page errors (actual JavaScript errors)
      page.on('pageerror', (error) => {
        consoleErrors.push(error.message);
      });

      // Navigate to the page
      await page.goto(`http://localhost:${E2E_PORT}/`, {
        waitUntil: 'networkidle',
      });

      // Wait a bit for any delayed scripts
      await page.waitForTimeout(1000);

      // Verify no JavaScript console errors
      expect(consoleErrors).toHaveLength(0);

      await page.close();
    }, 30000);

    it('should render the React app correctly in production build', async () => {
      if (!browser) {
        throw new Error('Browser not initialized');
      }

      const page: Page = await browser.newPage();

      await page.goto(`http://localhost:${E2E_PORT}/`, {
        waitUntil: 'networkidle',
      });

      // Check that the React root is populated
      const rootContent = await page.$eval('#root', (el) => el.innerHTML);
      expect(rootContent.length).toBeGreaterThan(0);

      // Check that the page has rendered content (not just an empty shell)
      const hasContent = await page.evaluate(() => {
        const root = document.getElementById('root');
        return root !== null && root.children.length > 0;
      });

      expect(hasContent).toBe(true);

      await page.close();
    }, 30000);
  });
});
