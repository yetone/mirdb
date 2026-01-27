/**
 * Integration tests for static site generation.
 * Owner: Scenario 16 - Static Site Generation
 *
 * Validates:
 * - Build completes successfully
 * - Static HTML output is generated
 * - No server-side dependencies
 * - Output is ready for GitHub Pages hosting
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { execSync, spawn, ChildProcess } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as http from 'http';

const PROJECT_ROOT = path.resolve(__dirname, '../..');
const DIST_DIR = path.join(PROJECT_ROOT, 'dist');

describe('Static Site Generation', () => {
  describe('Build Process', () => {
    beforeAll(() => {
      // Clean previous build
      if (fs.existsSync(DIST_DIR)) {
        fs.rmSync(DIST_DIR, { recursive: true });
      }
    });

    it('should complete build successfully', () => {
      // Test case 1: Run npm run build
      const result = execSync('npm run build', {
        cwd: PROJECT_ROOT,
        encoding: 'utf-8',
        timeout: 60000,
        stdio: ['pipe', 'pipe', 'pipe']
      });

      expect(result).toBeDefined();
      expect(fs.existsSync(DIST_DIR)).toBe(true);
    });

    it('should generate index.html in output directory', () => {
      // Test case 2: Verify output directory contains index.html
      const indexPath = path.join(DIST_DIR, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      const content = fs.readFileSync(indexPath, 'utf-8');
      expect(content).toContain('<!DOCTYPE html>');
      expect(content).toContain('<html');
      expect(content).toContain('</html>');
    });

    it('should generate CSS assets', () => {
      // Check for CSS files in the assets directory
      const assetsDir = path.join(DIST_DIR, '_assets');

      if (fs.existsSync(assetsDir)) {
        const files = fs.readdirSync(assetsDir, { recursive: true }) as string[];
        const cssFiles = files.filter(f => f.toString().endsWith('.css'));
        expect(cssFiles.length).toBeGreaterThan(0);
      } else {
        // CSS might be inlined, check index.html for style content
        const indexPath = path.join(DIST_DIR, 'index.html');
        const content = fs.readFileSync(indexPath, 'utf-8');
        expect(content).toMatch(/<style|<link[^>]*\.css/);
      }
    });

    it('should copy static assets to output', () => {
      // Check that public assets are copied
      const logoPath = path.join(DIST_DIR, 'assets', 'logo.gif');
      expect(fs.existsSync(logoPath)).toBe(true);
    });

    it('should not include server-side code markers', () => {
      // Test case 4: Verify no server-side dependencies
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf-8');

      // Static HTML should not contain these markers
      expect(content).not.toContain('__ASTRO_SSR__');
      expect(content).not.toContain('import.meta.env.SSR');

      // Should not have server-only imports in output
      const allFiles = getAllFiles(DIST_DIR);
      const jsFiles = allFiles.filter(f => f.endsWith('.js'));

      for (const jsFile of jsFiles) {
        const jsContent = fs.readFileSync(jsFile, 'utf-8');
        // Check for common server-side only modules that shouldn't appear in client bundle
        expect(jsContent).not.toContain("require('fs')");
        expect(jsContent).not.toContain("require('http')");
        expect(jsContent).not.toContain("require('path')");
      }
    });
  });

  describe('Static Serving', () => {
    let server: http.Server;
    let serverProcess: ChildProcess | null = null;
    const TEST_PORT = 4173;

    beforeAll(async () => {
      // Start a simple static file server
      server = http.createServer((req, res) => {
        const url = req.url === '/' ? '/index.html' : req.url;
        const filePath = path.join(DIST_DIR, url || '/index.html');

        if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
          const ext = path.extname(filePath);
          const contentTypes: Record<string, string> = {
            '.html': 'text/html',
            '.css': 'text/css',
            '.js': 'application/javascript',
            '.gif': 'image/gif',
            '.svg': 'image/svg+xml',
            '.json': 'application/json',
          };
          res.writeHead(200, { 'Content-Type': contentTypes[ext] || 'application/octet-stream' });
          fs.createReadStream(filePath).pipe(res);
        } else {
          res.writeHead(404);
          res.end('Not Found');
        }
      });

      await new Promise<void>((resolve) => {
        server.listen(TEST_PORT, () => resolve());
      });
    });

    afterAll(async () => {
      if (server) {
        await new Promise<void>((resolve) => {
          server.close(() => resolve());
        });
      }
      if (serverProcess) {
        serverProcess.kill();
      }
    });

    it('should serve index.html from static server', async () => {
      // Test case 3: Serve static files and load
      const response = await fetch(`http://localhost:${TEST_PORT}/`);
      expect(response.status).toBe(200);

      const html = await response.text();
      expect(html).toContain('<!DOCTYPE html>');
      expect(html).toContain('MirDB');
    });

    it('should serve CSS assets correctly', async () => {
      // Find a CSS file in assets
      const assetsDir = path.join(DIST_DIR, '_assets');

      if (fs.existsSync(assetsDir)) {
        const files = fs.readdirSync(assetsDir);
        const cssFile = files.find(f => f.endsWith('.css'));

        if (cssFile) {
          const response = await fetch(`http://localhost:${TEST_PORT}/_assets/${cssFile}`);
          expect(response.status).toBe(200);
          expect(response.headers.get('content-type')).toContain('text/css');
        }
      }
    });

    it('should serve logo asset correctly', async () => {
      const response = await fetch(`http://localhost:${TEST_PORT}/assets/logo.gif`);
      expect(response.status).toBe(200);
      expect(response.headers.get('content-type')).toBe('image/gif');
    });

    it('should work without Node.js runtime (no dynamic routes)', () => {
      // Verify all routes are pre-rendered
      const indexPath = path.join(DIST_DIR, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      // Check that there are no dynamic route files
      const allFiles = getAllFiles(DIST_DIR);
      const serverFiles = allFiles.filter(f =>
        f.includes('_server') ||
        f.includes('.mjs') ||
        f.endsWith('entry.mjs') ||
        f.includes('chunks/pages')
      );

      // Static output should not have server entry files
      expect(serverFiles.length).toBe(0);
    });
  });

  describe('GitHub Pages Compatibility', () => {
    it('should have proper file structure for GitHub Pages', () => {
      // GitHub Pages expects index.html at root
      expect(fs.existsSync(path.join(DIST_DIR, 'index.html'))).toBe(true);

      // Should have assets in a single assets directory (not nested node_modules)
      expect(fs.existsSync(path.join(DIST_DIR, 'node_modules'))).toBe(false);
    });

    it('should have all links as relative or absolute (not localhost)', () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf-8');

      // Should not contain localhost URL references in links/scripts/styles
      // Note: "localhost" may appear in content text (e.g., code examples like "telnet localhost")
      // but should not appear in href, src, or action attributes pointing to localhost
      expect(content).not.toMatch(/href=["']http:\/\/localhost/);
      expect(content).not.toMatch(/src=["']http:\/\/localhost/);
      expect(content).not.toMatch(/action=["']http:\/\/localhost/);
      expect(content).not.toMatch(/href=["']http:\/\/127\.0\.0\.1/);
      expect(content).not.toMatch(/src=["']http:\/\/127\.0\.0\.1/);
    });

    it('should generate valid HTML5 document', () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf-8');

      // Basic HTML5 structure validation
      expect(content).toMatch(/<!DOCTYPE html>/i);
      expect(content).toContain('<head>');
      expect(content).toContain('</head>');
      expect(content).toContain('<body');
      expect(content).toContain('</body>');
    });
  });
});

/**
 * Recursively get all files in a directory
 */
function getAllFiles(dir: string): string[] {
  const files: string[] = [];

  if (!fs.existsSync(dir)) {
    return files;
  }

  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...getAllFiles(fullPath));
    } else {
      files.push(fullPath);
    }
  }

  return files;
}
