import { test, expect } from '@playwright/test';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as http from 'http';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DIST_DIR = path.join(__dirname, '..', 'dist');
const HOMEPAGE_DIR = path.join(__dirname, '..');

test.describe('Static Site Build', () => {
  test.describe('Test Case 1: Build completes successfully', () => {
    test('build command runs without errors', async () => {
      // Clean dist directory first
      if (fs.existsSync(DIST_DIR)) {
        fs.rmSync(DIST_DIR, { recursive: true });
      }

      // Run build command
      let buildOutput: string;
      let buildExitCode = 0;
      try {
        buildOutput = execSync('npm run build', {
          cwd: HOMEPAGE_DIR,
          encoding: 'utf-8',
          stdio: ['pipe', 'pipe', 'pipe'],
        });
      } catch (error: any) {
        buildExitCode = error.status || 1;
        buildOutput = error.stdout || '';
        throw new Error(`Build failed with exit code ${buildExitCode}: ${error.stderr || error.message}`);
      }

      // Verify build completed
      expect(buildExitCode).toBe(0);
      expect(buildOutput).toContain('Complete!');
      expect(fs.existsSync(DIST_DIR)).toBe(true);
    });

    test('build generates dist directory', async () => {
      expect(fs.existsSync(DIST_DIR)).toBe(true);
      expect(fs.statSync(DIST_DIR).isDirectory()).toBe(true);
    });

    test('build output contains expected file count', async () => {
      const files = getAllFiles(DIST_DIR);
      // Should have at least index.html, 404.html, CSS, and favicon
      expect(files.length).toBeGreaterThanOrEqual(4);
    });
  });

  test.describe('Test Case 2: Build output contains required files', () => {
    test('output contains index.html', async () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      const content = fs.readFileSync(indexPath, 'utf-8');
      // Verify it's a valid HTML file
      expect(content).toContain('<!DOCTYPE html>');
      expect(content).toContain('<html');
      expect(content).toContain('</html>');
    });

    test('output contains 404.html for error handling', async () => {
      const errorPath = path.join(DIST_DIR, '404.html');
      expect(fs.existsSync(errorPath)).toBe(true);

      const content = fs.readFileSync(errorPath, 'utf-8');
      expect(content).toContain('<!DOCTYPE html>');
    });

    test('output contains CSS files', async () => {
      const astroDir = path.join(DIST_DIR, '_astro');
      expect(fs.existsSync(astroDir)).toBe(true);

      const files = fs.readdirSync(astroDir);
      const cssFiles = files.filter(f => f.endsWith('.css'));
      expect(cssFiles.length).toBeGreaterThan(0);

      // Verify CSS file has content
      const cssPath = path.join(astroDir, cssFiles[0]);
      const cssContent = fs.readFileSync(cssPath, 'utf-8');
      expect(cssContent.length).toBeGreaterThan(0);
    });

    test('output contains favicon', async () => {
      const faviconPath = path.join(DIST_DIR, 'favicon.svg');
      expect(fs.existsSync(faviconPath)).toBe(true);
    });

    test('index.html references CSS file', async () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf-8');

      // Check for CSS link tag
      expect(content).toMatch(/<link[^>]+rel=["']stylesheet["'][^>]*>/);
      expect(content).toContain('_astro');
      expect(content).toContain('.css');
    });

    test('index.html contains MirDB content', async () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf-8');

      // Verify key content is present
      expect(content).toContain('MirDB');
    });
  });

  test.describe('Test Case 3: Built site can be served', () => {
    let server: http.Server;
    const serverPort = 3456;

    // Helper function to make HTTP requests
    const httpGet = (urlPath: string): Promise<{ statusCode: number; body: string; headers: http.IncomingHttpHeaders }> => {
      return new Promise((resolve, reject) => {
        const req = http.get(`http://localhost:${serverPort}${urlPath}`, (res) => {
          let body = '';
          res.on('data', (chunk) => body += chunk);
          res.on('end', () => resolve({
            statusCode: res.statusCode || 0,
            body,
            headers: res.headers
          }));
        });
        req.on('error', reject);
        req.end();
      });
    };

    test.beforeAll(async () => {
      // Start a simple HTTP server for the dist directory
      server = http.createServer((req, res) => {
        let filePath = path.join(DIST_DIR, req.url === '/' ? 'index.html' : req.url!);

        if (!fs.existsSync(filePath)) {
          filePath = path.join(DIST_DIR, '404.html');
          res.statusCode = 404;
        }

        const ext = path.extname(filePath);
        const contentTypes: Record<string, string> = {
          '.html': 'text/html',
          '.css': 'text/css',
          '.js': 'application/javascript',
          '.svg': 'image/svg+xml',
        };

        res.setHeader('Content-Type', contentTypes[ext] || 'application/octet-stream');
        res.end(fs.readFileSync(filePath));
      });

      await new Promise<void>((resolve) => {
        server.listen(serverPort, resolve);
      });
    });

    test.afterAll(async () => {
      if (server) {
        await new Promise<void>((resolve) => {
          server.close(() => resolve());
        });
      }
    });

    test('static files can be served via HTTP', async () => {
      const response = await httpGet('/');

      // Page should load successfully with 200 status
      expect(response.statusCode).toBe(200);
      expect(response.headers['content-type']).toContain('text/html');
      expect(response.body).toBeTruthy();
    });

    test('served page renders with visible content', async () => {
      const response = await httpGet('/');

      // Page should contain valid HTML structure and MirDB content
      expect(response.body).toContain('<!DOCTYPE html>');
      expect(response.body).toContain('<html');
      expect(response.body).toContain('</html>');
      expect(response.body).toContain('MirDB');
      // Should have a body element
      expect(response.body).toContain('<body');
      expect(response.body).toContain('</body>');
    });

    test('CSS is properly loaded and applied', async () => {
      // First get the index.html to find the CSS file reference
      const htmlResponse = await httpGet('/');
      expect(htmlResponse.statusCode).toBe(200);

      // Extract CSS file path from the HTML
      const cssMatch = htmlResponse.body.match(/href="(\/_astro\/[^"]+\.css)"/);
      expect(cssMatch).toBeTruthy();

      // Fetch the CSS file
      const cssPath = cssMatch![1];
      const cssResponse = await httpGet(cssPath);

      expect(cssResponse.statusCode).toBe(200);
      expect(cssResponse.headers['content-type']).toContain('text/css');
      expect(cssResponse.body.length).toBeGreaterThan(0);
    });

    test('404 page is served for non-existent routes', async () => {
      const response = await httpGet('/non-existent-page');

      expect(response.statusCode).toBe(404);
      expect(response.body).toContain('<!DOCTYPE html>');
    });
  });
});

// Helper function to recursively get all files in a directory
function getAllFiles(dirPath: string, arrayOfFiles: string[] = []): string[] {
  const files = fs.readdirSync(dirPath);

  files.forEach(file => {
    const filePath = path.join(dirPath, file);
    if (fs.statSync(filePath).isDirectory()) {
      getAllFiles(filePath, arrayOfFiles);
    } else {
      arrayOfFiles.push(filePath);
    }
  });

  return arrayOfFiles;
}
