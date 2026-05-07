import { test, expect } from '@playwright/test';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as http from 'http';
import * as net from 'net';

const HOMEPAGE_DIR = path.resolve(__dirname, '../..');
const DIST_DIR = path.join(HOMEPAGE_DIR, 'dist');
const STATIC_PORT = 3456;
const STATIC_BASE_URL = `http://127.0.0.1:${STATIC_PORT}`;

const MIME_TYPES: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.gif': 'image/gif',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.txt': 'text/plain; charset=utf-8',
  '.ico': 'image/x-icon',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
};

function createStaticServer(rootDir: string): http.Server {
  return http.createServer((req, res) => {
    try {
      const urlPath = decodeURIComponent((req.url || '/').split('?')[0]);
      let filePath = path.join(rootDir, urlPath);

      if (filePath.endsWith(path.sep) || filePath === rootDir) {
        filePath = path.join(filePath, 'index.html');
      }

      if (!filePath.startsWith(rootDir)) {
        res.writeHead(403);
        res.end('Forbidden');
        return;
      }

      if (!fs.existsSync(filePath)) {
        const htmlFallback = filePath + '.html';
        if (fs.existsSync(htmlFallback)) {
          filePath = htmlFallback;
        } else {
          const notFoundPath = path.join(rootDir, '404.html');
          if (fs.existsSync(notFoundPath)) {
            res.writeHead(404, { 'Content-Type': 'text/html; charset=utf-8' });
            res.end(fs.readFileSync(notFoundPath));
            return;
          }
          res.writeHead(404);
          res.end('Not Found');
          return;
        }
      }

      const stat = fs.statSync(filePath);
      if (stat.isDirectory()) {
        const indexPath = path.join(filePath, 'index.html');
        if (fs.existsSync(indexPath)) {
          filePath = indexPath;
        } else {
          res.writeHead(404);
          res.end('Not Found');
          return;
        }
      }

      const ext = path.extname(filePath).toLowerCase();
      const contentType = MIME_TYPES[ext] || 'application/octet-stream';
      res.writeHead(200, { 'Content-Type': contentType });
      fs.createReadStream(filePath).pipe(res);
    } catch (err) {
      res.writeHead(500);
      res.end('Internal Server Error');
    }
  });
}

function listenPromise(server: http.Server, port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => resolve());
  });
}

function closePromise(server: http.Server): Promise<void> {
  return new Promise((resolve) => server.close(() => resolve()));
}

function listFilesRecursive(dir: string): string[] {
  const results: string[] = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...listFilesRecursive(fullPath));
    } else {
      results.push(fullPath);
    }
  }
  return results;
}

test.describe.configure({ mode: 'serial' });

let staticServer: http.Server | null = null;

test.describe('Static Site Generation', () => {
  test.beforeAll(async () => {
    if (!fs.existsSync(DIST_DIR)) {
      execSync('npm run build', {
        cwd: HOMEPAGE_DIR,
        stdio: 'pipe',
        env: { ...process.env, CI: '1' },
      });
    }
    staticServer = createStaticServer(DIST_DIR);
    await listenPromise(staticServer, STATIC_PORT);
  });

  test.afterAll(async () => {
    if (staticServer) {
      await closePromise(staticServer);
      staticServer = null;
    }
  });

  test('test case 1: build command execution completes without errors', async () => {
    const buildOutput = execSync('npm run build', {
      cwd: HOMEPAGE_DIR,
      stdio: 'pipe',
      env: { ...process.env, CI: '1' },
    }).toString();
    expect(buildOutput).toContain('Compiled successfully');
    expect(buildOutput).toMatch(/Generating static pages/);
    expect(fs.existsSync(DIST_DIR)).toBe(true);
  });

  test('test case 2: output directory contains static HTML, CSS, JS, asset files only', () => {
    expect(fs.existsSync(path.join(DIST_DIR, 'index.html'))).toBe(true);

    const indexHtml = fs.readFileSync(path.join(DIST_DIR, 'index.html'), 'utf-8');
    expect(indexHtml).toContain('<!DOCTYPE html>');
    expect(indexHtml.toLowerCase()).toContain('<html');

    expect(fs.existsSync(path.join(DIST_DIR, '_next', 'static'))).toBe(true);

    const cssDir = path.join(DIST_DIR, '_next', 'static', 'css');
    expect(fs.existsSync(cssDir)).toBe(true);
    const cssFiles = fs.readdirSync(cssDir).filter((f) => f.endsWith('.css'));
    expect(cssFiles.length).toBeGreaterThan(0);

    const chunksDir = path.join(DIST_DIR, '_next', 'static', 'chunks');
    expect(fs.existsSync(chunksDir)).toBe(true);
    const jsFiles = listFilesRecursive(chunksDir).filter((f) => f.endsWith('.js'));
    expect(jsFiles.length).toBeGreaterThan(0);

    const serverArtifacts = [
      'server',
      'middleware-manifest.json',
      'server-reference-manifest.json',
      'middleware.js',
      'BUILD_ID.server',
      'next-server.js',
    ];
    for (const artifact of serverArtifacts) {
      expect(fs.existsSync(path.join(DIST_DIR, artifact))).toBe(false);
    }

    const allFiles = listFilesRecursive(DIST_DIR);
    const allowedExtensions = new Set([
      '.html',
      '.css',
      '.js',
      '.json',
      '.txt',
      '.gif',
      '.png',
      '.jpg',
      '.jpeg',
      '.svg',
      '.ico',
      '.webp',
      '.avif',
      '.woff',
      '.woff2',
      '.map',
      '.xml',
      '.webmanifest',
      '',
    ]);
    for (const file of allFiles) {
      const ext = path.extname(file).toLowerCase();
      expect(allowedExtensions.has(ext)).toBe(true);
    }
  });

  test('test case 3: static file server serves index.html with HTTP 200', async () => {
    const response = await fetch(`${STATIC_BASE_URL}/`);
    expect(response.status).toBe(200);
    const body = await response.text();
    expect(body).toContain('<!DOCTYPE html>');
    expect(body).toContain('MirDB');

    const cssMatch = body.match(/href="(\/_next\/static\/css\/[^"]+\.css)"/);
    expect(cssMatch).not.toBeNull();
    const cssResponse = await fetch(`${STATIC_BASE_URL}${cssMatch![1]}`);
    expect(cssResponse.status).toBe(200);
    expect(cssResponse.headers.get('content-type')).toContain('text/css');

    const jsMatch = body.match(/src="(\/_next\/static\/chunks\/[^"]+\.js)"/);
    expect(jsMatch).not.toBeNull();
    const jsResponse = await fetch(`${STATIC_BASE_URL}${jsMatch![1]}`);
    expect(jsResponse.status).toBe(200);
    expect(jsResponse.headers.get('content-type')).toContain('javascript');
  });

  test('test case 3b: page renders in browser when served statically', async ({ page }) => {
    const failedRequests: { url: string; status: number }[] = [];
    page.on('response', (resp) => {
      if (resp.status() >= 400) {
        failedRequests.push({ url: resp.url(), status: resp.status() });
      }
    });

    const consoleErrors: string[] = [];
    page.on('pageerror', (err) => consoleErrors.push(err.message));

    await page.goto(STATIC_BASE_URL, { waitUntil: 'networkidle' });

    await expect(page).toHaveTitle(/MirDB/);
    await expect(page.locator('body')).toBeVisible();

    expect(failedRequests).toEqual([]);
    expect(consoleErrors).toEqual([]);
  });

  test('test case 4: no API routes or server-only endpoints exist in output', async () => {
    const apiPaths = ['/api', '/api/health', '/api/users', '/api/data'];
    for (const apiPath of apiPaths) {
      const response = await fetch(`${STATIC_BASE_URL}${apiPath}`);
      expect(response.status).toBe(404);
    }

    expect(fs.existsSync(path.join(DIST_DIR, 'api'))).toBe(false);

    const indexHtml = fs.readFileSync(path.join(DIST_DIR, 'index.html'), 'utf-8');
    expect(indexHtml).not.toContain('"isFallback":true');
    expect(indexHtml).not.toMatch(/getServerSideProps/);

    const allFiles = listFilesRecursive(DIST_DIR);
    const serverOnlyPatterns = [/server\.js$/, /pages-manifest\.json$/, /next-server/];
    for (const file of allFiles) {
      for (const pattern of serverOnlyPatterns) {
        expect(pattern.test(file)).toBe(false);
      }
    }
  });

  test('test case 4b: no Node.js runtime is required to render the page', () => {
    const allFiles = listFilesRecursive(DIST_DIR);
    expect(allFiles.length).toBeGreaterThan(0);
    for (const file of allFiles) {
      const ext = path.extname(file).toLowerCase();
      expect(ext).not.toBe('.node');
      expect(ext).not.toBe('.cjs');
    }

    expect(fs.existsSync(path.join(DIST_DIR, 'package.json'))).toBe(false);
    expect(fs.existsSync(path.join(DIST_DIR, 'node_modules'))).toBe(false);
  });
});
