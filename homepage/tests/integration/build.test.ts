/**
 * Integration tests for build output, bundle sizes, and static site generation.
 * Owners:
 *   - Scenario 13 - Performance Optimization (bundle size tests)
 *   - Scenario 20 - Static Site Generation Build (static export tests)
 *
 * Tests:
 * - CSS bundle is gzipped and under 50KB
 * - JavaScript bundle is code-split and main bundle under 100KB gzipped
 * - Static HTML files are generated in out/ directory
 * - Assets are hashed for cache busting
 * - Site can be served statically without runtime dependencies
 *
 * Requirements: REQ-11, NFR-2, NFR-4
 */

import { execSync, spawn, ChildProcess } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as zlib from 'zlib';
import * as http from 'http';

const PROJECT_ROOT = path.resolve(__dirname, '../..');
const NEXT_DIR = path.join(PROJECT_ROOT, '.next');
const STATIC_DIR = path.join(NEXT_DIR, 'static');
const OUT_DIR = path.join(PROJECT_ROOT, 'out');

// Helper to calculate gzipped size in bytes
function getGzippedSize(filePath: string): number {
  const content = fs.readFileSync(filePath);
  const gzipped = zlib.gzipSync(content);
  return gzipped.length;
}

// Helper to convert bytes to KB
function bytesToKB(bytes: number): number {
  return bytes / 1024;
}

// Helper to find files recursively matching a pattern
function findFiles(dir: string, pattern: RegExp): string[] {
  const results: string[] = [];

  if (!fs.existsSync(dir)) {
    return results;
  }

  const items = fs.readdirSync(dir, { withFileTypes: true });

  for (const item of items) {
    const fullPath = path.join(dir, item.name);
    if (item.isDirectory()) {
      results.push(...findFiles(fullPath, pattern));
    } else if (pattern.test(item.name)) {
      results.push(fullPath);
    }
  }

  return results;
}

describe('Build Output Tests', () => {
  beforeAll(() => {
    // Run build before tests
    console.log('Building Next.js project...');
    try {
      execSync('npm run build', {
        cwd: PROJECT_ROOT,
        stdio: 'pipe',
        timeout: 120000, // 2 minute timeout
      });
    } catch (error) {
      console.error('Build failed:', error);
      throw error;
    }
  }, 180000); // 3 minute timeout for beforeAll

  describe('CSS Bundle Size', () => {
    it('should have CSS bundle gzipped and under 50KB', () => {
      // Find all CSS files in the build output
      const cssFiles = findFiles(STATIC_DIR, /\.css$/);

      expect(cssFiles.length).toBeGreaterThan(0);

      let totalGzippedSize = 0;
      const cssFileSizes: { file: string; gzippedKB: number }[] = [];

      for (const cssFile of cssFiles) {
        const gzippedSize = getGzippedSize(cssFile);
        totalGzippedSize += gzippedSize;
        cssFileSizes.push({
          file: path.basename(cssFile),
          gzippedKB: bytesToKB(gzippedSize),
        });
      }

      const totalGzippedKB = bytesToKB(totalGzippedSize);

      console.log('CSS Bundle Analysis:');
      cssFileSizes.forEach(({ file, gzippedKB }) => {
        console.log(`  ${file}: ${gzippedKB.toFixed(2)} KB gzipped`);
      });
      console.log(`  Total CSS: ${totalGzippedKB.toFixed(2)} KB gzipped`);

      // CSS bundle should be under 50KB gzipped
      expect(totalGzippedKB).toBeLessThan(50);
    });
  });

  describe('JavaScript Bundle Size', () => {
    it('should have code-split JavaScript with main bundle under 100KB gzipped', () => {
      // Find all JS chunks in the build output
      const jsChunksDir = path.join(STATIC_DIR, 'chunks');
      const jsFiles = findFiles(jsChunksDir, /\.js$/);

      expect(jsFiles.length).toBeGreaterThan(0);

      // Code splitting verification: should have multiple chunks
      expect(jsFiles.length).toBeGreaterThan(1);

      const jsFileSizes: { file: string; gzippedKB: number }[] = [];
      let mainBundleSize = 0;

      for (const jsFile of jsFiles) {
        const gzippedSize = getGzippedSize(jsFile);
        const fileName = path.basename(jsFile);

        jsFileSizes.push({
          file: fileName,
          gzippedKB: bytesToKB(gzippedSize),
        });

        // Identify main bundle - typically the largest app chunk or main-*.js
        // Next.js uses patterns like main-*, app-*, webpack-*
        if (fileName.startsWith('main-') || fileName.includes('main.')) {
          mainBundleSize = Math.max(mainBundleSize, gzippedSize);
        }
      }

      // Sort by size descending
      jsFileSizes.sort((a, b) => b.gzippedKB - a.gzippedKB);

      console.log('JavaScript Bundle Analysis:');
      console.log(`  Total chunks: ${jsFiles.length} (code-splitting verified)`);
      jsFileSizes.slice(0, 10).forEach(({ file, gzippedKB }) => {
        console.log(`  ${file}: ${gzippedKB.toFixed(2)} KB gzipped`);
      });

      // If no explicit main bundle found, use the largest chunk
      if (mainBundleSize === 0 && jsFileSizes.length > 0) {
        mainBundleSize = jsFileSizes[0].gzippedKB * 1024;
      }

      const mainBundleKB = bytesToKB(mainBundleSize);
      console.log(`  Main bundle: ${mainBundleKB.toFixed(2)} KB gzipped`);

      // Main bundle should be under 100KB gzipped
      expect(mainBundleKB).toBeLessThan(100);
    });

    it('should verify JavaScript is minified', () => {
      const jsFiles = findFiles(STATIC_DIR, /\.js$/);

      expect(jsFiles.length).toBeGreaterThan(0);

      // Check a sample of JS files for minification characteristics
      for (const jsFile of jsFiles.slice(0, 5)) {
        const content = fs.readFileSync(jsFile, 'utf-8');

        // Minified JS typically has:
        // - Few or no newlines relative to content length
        // - Short variable names
        // - No multi-line comments (except source maps)
        const lines = content.split('\n').filter(line =>
          line.trim() && !line.trim().startsWith('//')
        );

        // Minified files should have high content density (characters per line)
        if (content.length > 1000) {
          const avgLineLength = content.length / lines.length;
          // Minified code typically has very long lines (>100 chars average)
          expect(avgLineLength).toBeGreaterThan(50);
        }
      }
    });
  });

  describe('CSS Optimization', () => {
    it('should verify CSS is minified', () => {
      const cssFiles = findFiles(STATIC_DIR, /\.css$/);

      expect(cssFiles.length).toBeGreaterThan(0);

      for (const cssFile of cssFiles) {
        const content = fs.readFileSync(cssFile, 'utf-8');

        // Minified CSS characteristics:
        // - Minimal whitespace
        // - No multi-line comments (except source maps)
        // - High character density per line
        if (content.length > 500) {
          const lines = content.split('\n').filter(line => line.trim());
          const avgLineLength = content.length / Math.max(lines.length, 1);

          // Minified CSS typically has long lines
          expect(avgLineLength).toBeGreaterThan(50);
        }
      }
    });
  });
});

/**
 * Static Site Generation Tests
 * Owner: Scenario 20 - Static Site Generation Build
 *
 * Tests verify the site builds correctly as a static site with optimal output:
 * - Build completes successfully
 * - Static HTML index.html is generated
 * - CSS and JS files are minified and hashed for cache busting
 * - Site serves correctly from static file server
 *
 * Requirements: NFR-4
 */
describe('Static Site Generation Tests', () => {
  beforeAll(() => {
    // Build should have already been run by the first test suite
    // If out/ directory doesn't exist, run build
    if (!fs.existsSync(OUT_DIR)) {
      console.log('Building Next.js static export...');
      try {
        execSync('npm run build', {
          cwd: PROJECT_ROOT,
          stdio: 'pipe',
          timeout: 120000,
        });
      } catch (error) {
        console.error('Build failed:', error);
        throw error;
      }
    }
  }, 180000);

  describe('Build Success', () => {
    it('should complete production build successfully with no errors', () => {
      // Verify build completed by checking that out/ directory exists
      expect(fs.existsSync(OUT_DIR)).toBe(true);

      // Verify it's a directory
      const stats = fs.statSync(OUT_DIR);
      expect(stats.isDirectory()).toBe(true);

      console.log('Build completed successfully - out/ directory exists');
    });
  });

  describe('Static HTML Generation', () => {
    it('should generate index.html in the output directory', () => {
      const indexPath = path.join(OUT_DIR, 'index.html');

      expect(fs.existsSync(indexPath)).toBe(true);

      // Verify it's a valid HTML file
      const content = fs.readFileSync(indexPath, 'utf-8');
      expect(content).toContain('<!DOCTYPE html>');
      expect(content).toContain('<html');
      expect(content).toContain('</html>');

      // Verify it contains the app content (not just an empty shell)
      expect(content).toContain('MirDB');

      console.log('Static HTML index.html generated successfully');
    });

    it('should generate self-contained HTML with embedded data', () => {
      const indexPath = path.join(OUT_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf-8');

      // Static export should have the page content pre-rendered
      // Check for key sections that should be server-rendered
      expect(content).toMatch(/<body[^>]*>/);
      expect(content).toMatch(/<head[^>]*>/);

      // Verify script and style references exist
      expect(content).toMatch(/<script[^>]*>/);
      expect(content).toMatch(/<link[^>]*rel="stylesheet"/);
    });
  });

  describe('Asset Optimization', () => {
    it('should generate CSS files with hashed filenames for cache busting', () => {
      const nextStaticDir = path.join(OUT_DIR, '_next', 'static');

      // Find CSS files
      const cssFiles = findFiles(nextStaticDir, /\.css$/);
      expect(cssFiles.length).toBeGreaterThan(0);

      // Verify hashed filenames (pattern: name-hash.css or hash.css)
      const hashPattern = /[a-f0-9]{8,}/i;
      const hashedFiles = cssFiles.filter(f => hashPattern.test(path.basename(f)));

      console.log(`Found ${cssFiles.length} CSS files, ${hashedFiles.length} with hashed names`);
      expect(hashedFiles.length).toBeGreaterThan(0);

      // Verify CSS is minified
      for (const cssFile of cssFiles) {
        const content = fs.readFileSync(cssFile, 'utf-8');
        if (content.length > 100) {
          // Minified CSS has few newlines relative to content
          const newlineRatio = content.split('\n').length / content.length;
          expect(newlineRatio).toBeLessThan(0.05); // Less than 5% newlines
        }
      }
    });

    it('should generate JS files with hashed filenames for cache busting', () => {
      const nextStaticDir = path.join(OUT_DIR, '_next', 'static');

      // Find JS files
      const jsFiles = findFiles(nextStaticDir, /\.js$/);
      expect(jsFiles.length).toBeGreaterThan(0);

      // Verify hashed filenames
      const hashPattern = /[a-f0-9]{8,}/i;
      const hashedFiles = jsFiles.filter(f => hashPattern.test(path.basename(f)));

      console.log(`Found ${jsFiles.length} JS files, ${hashedFiles.length} with hashed names`);
      expect(hashedFiles.length).toBeGreaterThan(0);

      // Verify JS is minified (sample check)
      const sampleFile = jsFiles[0];
      const content = fs.readFileSync(sampleFile, 'utf-8');
      if (content.length > 500) {
        // Minified JS has high character density per line
        const lines = content.split('\n').filter(l => l.trim());
        const avgLineLength = content.length / Math.max(lines.length, 1);
        expect(avgLineLength).toBeGreaterThan(50);
      }
    });
  });

  describe('Static Serving Capability', () => {
    let server: http.Server | null = null;
    let currentPort = 3456;

    const closeServer = (): Promise<void> => {
      return new Promise((resolve) => {
        if (server) {
          server.close(() => {
            server = null;
            resolve();
          });
        } else {
          resolve();
        }
      });
    };

    afterEach(async () => {
      await closeServer();
      // Increment port for next test to avoid conflicts
      currentPort++;
    });

    it('should serve correctly from a static file server without runtime dependencies', (done) => {
      // Create a simple static file server to test the build output
      const handler = (req: http.IncomingMessage, res: http.ServerResponse) => {
        let filePath = path.join(OUT_DIR, req.url === '/' ? 'index.html' : req.url || '');

        // Handle trailing slash by appending index.html
        if (fs.existsSync(filePath) && fs.statSync(filePath).isDirectory()) {
          filePath = path.join(filePath, 'index.html');
        }

        if (!fs.existsSync(filePath)) {
          res.writeHead(404);
          res.end('Not Found');
          return;
        }

        const ext = path.extname(filePath).toLowerCase();
        const contentTypes: Record<string, string> = {
          '.html': 'text/html',
          '.css': 'text/css',
          '.js': 'application/javascript',
          '.json': 'application/json',
          '.svg': 'image/svg+xml',
          '.png': 'image/png',
          '.ico': 'image/x-icon',
        };

        const contentType = contentTypes[ext] || 'application/octet-stream';
        const content = fs.readFileSync(filePath);

        res.writeHead(200, { 'Content-Type': contentType });
        res.end(content);
      };

      server = http.createServer(handler);

      server.listen(currentPort, () => {
        // Make a request to the static server
        http.get(`http://localhost:${currentPort}/`, (res) => {
          let data = '';

          res.on('data', (chunk) => {
            data += chunk;
          });

          res.on('end', () => {
            // Verify successful response
            expect(res.statusCode).toBe(200);
            expect(res.headers['content-type']).toContain('text/html');

            // Verify page content is present
            expect(data).toContain('<!DOCTYPE html>');
            expect(data).toContain('MirDB');

            console.log('Static site serves correctly without runtime dependencies');
            done();
          });
        }).on('error', (err) => {
          done(err);
        });
      });
    }, 30000);

    it('should serve static assets (CSS, JS) correctly', (done) => {
      const handler = (req: http.IncomingMessage, res: http.ServerResponse) => {
        let filePath = path.join(OUT_DIR, req.url || '');

        if (!fs.existsSync(filePath)) {
          res.writeHead(404);
          res.end('Not Found');
          return;
        }

        const ext = path.extname(filePath).toLowerCase();
        const contentTypes: Record<string, string> = {
          '.css': 'text/css',
          '.js': 'application/javascript',
        };

        const contentType = contentTypes[ext] || 'application/octet-stream';
        const content = fs.readFileSync(filePath);

        res.writeHead(200, { 'Content-Type': contentType });
        res.end(content);
      };

      server = http.createServer(handler);

      server.listen(currentPort, () => {
        // Find a JS file to test
        const nextStaticDir = path.join(OUT_DIR, '_next', 'static');
        const jsFiles = findFiles(nextStaticDir, /\.js$/);

        if (jsFiles.length === 0) {
          done(new Error('No JS files found in build output'));
          return;
        }

        // Get relative path from OUT_DIR
        const testFile = jsFiles[0];
        const relativePath = path.relative(OUT_DIR, testFile);

        http.get(`http://localhost:${currentPort}/${relativePath}`, (res) => {
          expect(res.statusCode).toBe(200);
          expect(res.headers['content-type']).toContain('application/javascript');

          let data = '';
          res.on('data', (chunk) => {
            data += chunk;
          });

          res.on('end', () => {
            // Verify it's valid JS content (not empty, not HTML)
            expect(data.length).toBeGreaterThan(0);
            expect(data).not.toContain('<!DOCTYPE html>');

            console.log('Static assets serve correctly');
            done();
          });
        }).on('error', (err) => {
          done(err);
        });
      });
    }, 30000);
  });
});
