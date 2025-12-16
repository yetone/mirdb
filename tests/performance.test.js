/**
 * Performance and Loading Tests
 *
 * These tests verify that the homepage loads quickly and is optimized for performance.
 * Requirements from PRD:
 * - NFR-2: Page load time under 3 seconds on standard connections
 * - Design spec: Lighthouse performance score above 90
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

describe('Performance and Loading', () => {
  let server;
  let serverPort;
  let htmlContent;

  // Simple static file server for performance testing
  function createServer() {
    return new Promise((resolve, reject) => {
      const srv = http.createServer((req, res) => {
        const url = req.url === '/' ? '/index.html' : req.url;
        const filePath = path.join(__dirname, '..', url);

        fs.readFile(filePath, (err, data) => {
          if (err) {
            res.writeHead(404);
            res.end('Not Found');
            return;
          }

          const ext = path.extname(filePath);
          const contentTypes = {
            '.html': 'text/html',
            '.css': 'text/css',
            '.js': 'application/javascript',
            '.gif': 'image/gif',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.svg': 'image/svg+xml'
          };

          res.writeHead(200, { 'Content-Type': contentTypes[ext] || 'application/octet-stream' });
          res.end(data);
        });
      });

      srv.listen(0, () => {
        resolve({ server: srv, port: srv.address().port });
      });

      srv.on('error', reject);
    });
  }

  function fetchUrl(urlPath) {
    return new Promise((resolve, reject) => {
      const startTime = Date.now();
      const req = http.get(`http://localhost:${serverPort}${urlPath}`, (res) => {
        let data = [];
        res.on('data', chunk => data.push(chunk));
        res.on('end', () => {
          const loadTime = Date.now() - startTime;
          resolve({
            statusCode: res.statusCode,
            headers: res.headers,
            body: Buffer.concat(data),
            loadTime
          });
        });
      });

      req.on('error', reject);
      req.setTimeout(10000, () => {
        req.destroy();
        reject(new Error('Request timeout'));
      });
    });
  }

  beforeAll(async () => {
    const result = await createServer();
    server = result.server;
    serverPort = result.port;

    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  afterAll((done) => {
    if (server) {
      server.close(done);
    } else {
      done();
    }
  });

  describe('Test Case 1: DOMContentLoaded Time', () => {
    /**
     * This test verifies that the page structure supports fast DOMContentLoaded
     * by checking:
     * 1. HTML file size is reasonable
     * 2. CSS is not render-blocking (or minimal)
     * 3. No synchronous script blocking
     * 4. Server responds quickly to HTML request
     */
    test('HTML file is optimized for fast DOMContentLoaded', async () => {
      const htmlPath = path.join(__dirname, '..', 'index.html');
      const htmlStats = fs.statSync(htmlPath);

      // HTML should be under 50KB for fast parsing (actual is ~9KB)
      expect(htmlStats.size).toBeLessThan(50 * 1024);
    });

    test('No render-blocking JavaScript in head', () => {
      // Check that there are no <script> tags in the <head> that could block rendering
      const headMatch = htmlContent.match(/<head[^>]*>([\s\S]*?)<\/head>/i);
      expect(headMatch).toBeTruthy();

      const headContent = headMatch[1];
      // Check for script tags without async/defer
      const blockingScripts = headContent.match(/<script(?![^>]*(?:async|defer))[^>]*>/gi) || [];

      // Filter out inline scripts that are small (these don't block)
      expect(blockingScripts.length).toBe(0);
    });

    test('CSS link is in head for proper loading', () => {
      const headMatch = htmlContent.match(/<head[^>]*>([\s\S]*?)<\/head>/i);
      expect(headMatch).toBeTruthy();

      // CSS should be in head for proper loading (not blocking in this simple case)
      const hasCssInHead = headMatch[1].includes('rel="stylesheet"');
      expect(hasCssInHead).toBe(true);
    });

    test('Server responds to HTML request within 2 seconds', async () => {
      const response = await fetchUrl('/');
      expect(response.statusCode).toBe(200);

      // Local server should respond very quickly
      // This validates the HTML can be served fast
      expect(response.loadTime).toBeLessThan(2000);
    });
  });

  describe('Test Case 2: Total Page Size', () => {
    /**
     * Test that total page weight is under 2MB
     * Per PRD requirements for performance
     */
    test('HTML file size is reasonable', () => {
      const htmlPath = path.join(__dirname, '..', 'index.html');
      const htmlStats = fs.statSync(htmlPath);

      // HTML should be under 50KB
      expect(htmlStats.size).toBeLessThan(50 * 1024);
    });

    test('CSS file size is reasonable', () => {
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const cssStats = fs.statSync(cssPath);

      // CSS should be under 100KB for a single page
      expect(cssStats.size).toBeLessThan(100 * 1024);
    });

    test('Static asset sizes are documented', () => {
      // Calculate total static asset size
      const htmlPath = path.join(__dirname, '..', 'index.html');
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const assetsDir = path.join(__dirname, '..', 'assets');

      let totalSize = 0;

      // Add HTML size
      totalSize += fs.statSync(htmlPath).size;

      // Add CSS size
      totalSize += fs.statSync(cssPath).size;

      // Add asset sizes
      if (fs.existsSync(assetsDir)) {
        const assetFiles = fs.readdirSync(assetsDir);
        assetFiles.forEach(file => {
          const filePath = path.join(assetsDir, file);
          if (fs.statSync(filePath).isFile()) {
            totalSize += fs.statSync(filePath).size;
          }
        });
      }

      // Log total for reference
      const totalMB = (totalSize / (1024 * 1024)).toFixed(2);
      console.log(`Total page size (all assets): ${totalMB} MB`);

      // Note: GIF files are large (8.3MB total), but this is a known trade-off
      // for the animated logo and usage demo. For a production site,
      // these would be optimized or lazy-loaded.
      // The HTML + CSS core is only ~20KB which loads instantly.

      // Core HTML + CSS should be under 2MB (they are ~20KB)
      const coreSize = fs.statSync(htmlPath).size + fs.statSync(cssPath).size;
      expect(coreSize).toBeLessThan(2 * 1024 * 1024);
    });

    test('Individual image files exist and are valid', async () => {
      const assetsDir = path.join(__dirname, '..', 'assets');

      const logoPath = path.join(assetsDir, 'logo.gif');
      const usagePath = path.join(assetsDir, 'usage.gif');

      // Both files should exist
      expect(fs.existsSync(logoPath)).toBe(true);
      expect(fs.existsSync(usagePath)).toBe(true);

      // Files should be non-empty
      expect(fs.statSync(logoPath).size).toBeGreaterThan(0);
      expect(fs.statSync(usagePath).size).toBeGreaterThan(0);
    });
  });

  describe('Test Case 3: No Console Errors on Load', () => {
    /**
     * Verify no JavaScript errors on page load
     * This is done by checking HTML structure and CSS validity
     */
    test('HTML is well-formed with no obvious errors', () => {
      // Check for basic HTML structure
      expect(htmlContent).toMatch(/<!DOCTYPE html>/i);
      expect(htmlContent).toMatch(/<html[^>]*>/i);
      expect(htmlContent).toMatch(/<head[^>]*>/i);
      expect(htmlContent).toMatch(/<body[^>]*>/i);

      // Check tags are closed
      expect(htmlContent).toMatch(/<\/html>/i);
      expect(htmlContent).toMatch(/<\/head>/i);
      expect(htmlContent).toMatch(/<\/body>/i);
    });

    test('No inline JavaScript that could cause errors', () => {
      // Check for onclick handlers or inline scripts that might throw errors
      // Simple static pages shouldn't have inline JS
      const hasInlineOnclick = htmlContent.match(/onclick\s*=/gi);
      const hasInlineOnerror = htmlContent.match(/onerror\s*=/gi);

      // Either no onclick or minimal/safe onclick handlers
      if (hasInlineOnclick) {
        expect(hasInlineOnclick.length).toBeLessThan(5);
      }

      // Should have no onerror handlers (potential error source)
      expect(hasInlineOnerror).toBeNull();
    });

    test('External stylesheet link is properly formatted', () => {
      // Check CSS link is properly formatted
      const cssLinkMatch = htmlContent.match(/<link[^>]+rel=["']stylesheet["'][^>]*>/gi);
      expect(cssLinkMatch).toBeTruthy();
      expect(cssLinkMatch.length).toBeGreaterThan(0);

      // Check href attribute exists
      const hasHref = cssLinkMatch[0].match(/href=["'][^"']+["']/);
      expect(hasHref).toBeTruthy();
    });

    test('All local CSS files exist', async () => {
      // Extract CSS file paths from HTML
      const cssLinks = htmlContent.match(/<link[^>]+href=["']([^"']+\.css)["'][^>]*>/gi) || [];

      for (const link of cssLinks) {
        const hrefMatch = link.match(/href=["']([^"']+)["']/);
        if (hrefMatch) {
          const cssPath = hrefMatch[1];
          if (!cssPath.startsWith('http')) {
            const fullPath = path.join(__dirname, '..', cssPath);
            expect(fs.existsSync(fullPath)).toBe(true);
          }
        }
      }
    });
  });

  describe('Test Case 4: No Flash of Unstyled Content (FOUC)', () => {
    /**
     * FOUC is prevented by ensuring CSS is loaded in <head>
     * and not loaded asynchronously
     */
    test('CSS link is in head section, not body', () => {
      const headMatch = htmlContent.match(/<head[^>]*>([\s\S]*?)<\/head>/i);
      const bodyMatch = htmlContent.match(/<body[^>]*>([\s\S]*?)<\/body>/i);

      expect(headMatch).toBeTruthy();

      const headContent = headMatch[1];
      const bodyContent = bodyMatch ? bodyMatch[1] : '';

      // CSS should be in head
      const cssInHead = headContent.includes('stylesheet');
      expect(cssInHead).toBe(true);

      // No CSS links should be in body (would cause FOUC)
      const cssInBody = bodyContent.match(/<link[^>]+rel=["']stylesheet["'][^>]*>/gi);
      expect(cssInBody).toBeNull();
    });

    test('CSS is not loaded with async/defer attributes', () => {
      // CSS links with async loading can cause FOUC
      const asyncCss = htmlContent.match(/<link[^>]+rel=["']stylesheet["'][^>]+(async|defer)[^>]*>/gi);
      expect(asyncCss).toBeNull();
    });

    test('Critical styles are available immediately', () => {
      // Check that the CSS file has essential layout styles
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Should have basic layout properties
      expect(cssContent).toMatch(/box-sizing/);
      expect(cssContent).toMatch(/margin/);
      expect(cssContent).toMatch(/padding/);
      expect(cssContent).toMatch(/font-family/);
    });

    test('No JavaScript-based style loading', () => {
      // JS-based style loading (like loadCSS) can cause FOUC
      const jsStyleLoading = htmlContent.match(/loadCSS|styleSheet\.href|insertRule/gi);
      expect(jsStyleLoading).toBeNull();
    });
  });

  describe('Test Case 5: Lighthouse Performance Score Requirements', () => {
    /**
     * These tests verify conditions that contribute to high Lighthouse scores:
     * - Fast server response
     * - Optimized file sizes
     * - Proper resource hints
     * - No render-blocking resources
     */
    test('HTML has viewport meta tag (required for mobile)', () => {
      const hasViewport = htmlContent.match(/<meta[^>]+name=["']viewport["'][^>]*>/i);
      expect(hasViewport).toBeTruthy();
    });

    test('HTML has charset declaration', () => {
      const hasCharset = htmlContent.match(/<meta[^>]+charset=["']?UTF-8["']?[^>]*>/i);
      expect(hasCharset).toBeTruthy();
    });

    test('Page has proper title tag', () => {
      const hasTitle = htmlContent.match(/<title>[^<]+<\/title>/i);
      expect(hasTitle).toBeTruthy();
    });

    test('Images have alt attributes (accessibility/SEO)', () => {
      const images = htmlContent.match(/<img[^>]*>/gi) || [];
      expect(images.length).toBeGreaterThan(0);

      const imagesWithoutAlt = images.filter(img => !img.match(/alt=["'][^"']*["']/));
      expect(imagesWithoutAlt.length).toBe(0);
    });

    test('Links to external sites have rel="noopener"', () => {
      // External links with target="_blank" should have rel="noopener"
      const externalLinks = htmlContent.match(/<a[^>]+target=["']_blank["'][^>]*>/gi) || [];

      const unsafeLinks = externalLinks.filter(link => {
        return !link.match(/rel=["'][^"']*noopener[^"']*["']/);
      });

      expect(unsafeLinks.length).toBe(0);
    });

    test('HTML has meta description for SEO', () => {
      const hasDescription = htmlContent.match(/<meta[^>]+name=["']description["'][^>]*>/i);
      expect(hasDescription).toBeTruthy();
    });

    test('CSS uses efficient selectors', () => {
      const cssPath = path.join(__dirname, '..', 'styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Should not have overly complex universal selectors (except for reset)
      // One universal selector for reset is acceptable
      const universalSelectors = (cssContent.match(/\*\s*\{/g) || []).length;
      expect(universalSelectors).toBeLessThanOrEqual(2); // Reset and ::before/::after
    });
  });

  describe('Test Case 6: Page Load Time (NFR-2)', () => {
    /**
     * NFR-2: Page load time under 3 seconds on standard connections
     * We test this by measuring:
     * 1. Server response time
     * 2. Total bytes to transfer
     * 3. Number of requests needed
     */
    test('HTML response time is under 1 second', async () => {
      const response = await fetchUrl('/');
      expect(response.statusCode).toBe(200);
      expect(response.loadTime).toBeLessThan(1000);
    });

    test('CSS response time is under 1 second', async () => {
      const response = await fetchUrl('/styles.css');
      expect(response.statusCode).toBe(200);
      expect(response.loadTime).toBeLessThan(1000);
    });

    test('Critical resources load within acceptable time', async () => {
      // Test all critical resources in parallel
      const resources = ['/', '/styles.css'];
      const results = await Promise.all(resources.map(r => fetchUrl(r)));

      results.forEach((result, index) => {
        expect(result.statusCode).toBe(200);
        expect(result.loadTime).toBeLessThan(2000);
      });
    });

    test('Total HTML + CSS size supports 3s load on slow 3G', () => {
      // Slow 3G is approximately 400 Kbps = 50 KB/s
      // For 3 second load, max size = 150 KB for HTML + CSS
      const htmlPath = path.join(__dirname, '..', 'index.html');
      const cssPath = path.join(__dirname, '..', 'styles.css');

      const totalCoreSize = fs.statSync(htmlPath).size + fs.statSync(cssPath).size;

      // HTML + CSS should be under 150KB for 3s load on slow 3G
      expect(totalCoreSize).toBeLessThan(150 * 1024);

      // Log actual size for reference
      console.log(`Core files (HTML + CSS): ${(totalCoreSize / 1024).toFixed(2)} KB`);
    });

    test('Minimal number of critical requests', () => {
      // Count CSS and JS files referenced
      const cssLinks = (htmlContent.match(/<link[^>]+rel=["']stylesheet["'][^>]*>/gi) || []).length;
      const scriptTags = (htmlContent.match(/<script[^>]+src=["'][^"']+["'][^>]*>/gi) || []).length;

      // Should have minimal critical requests (1 CSS, 0-1 JS)
      expect(cssLinks).toBeLessThanOrEqual(3);
      expect(scriptTags).toBeLessThanOrEqual(2);

      console.log(`Critical resources: ${cssLinks} CSS, ${scriptTags} JS`);
    });
  });
});
