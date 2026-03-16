/**
 * Static Site Requirements Tests
 * Owner: Scenario 15 - Static Site Requirements
 *
 * Test cases:
 * - index.html exists in docs directory
 * - No API calls on page load
 * - Works with simple HTTP server
 * - All asset paths are relative
 *
 * This validates NFR-6: Static site with no backend dependencies for hosting
 */

import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

const rootDir = path.resolve(__dirname, '../..');
const docsDir = path.join(rootDir, 'docs');

test.describe('Static Site Requirements (NFR-6)', () => {
  test.describe('TC1: Static HTML file structure', () => {
    test('index.html exists in the docs directory', async () => {
      const indexPath = path.join(docsDir, 'index.html');
      const exists = fs.existsSync(indexPath);
      expect(exists).toBe(true);
    });

    test('site consists only of static files (HTML, CSS, JS, images)', async () => {
      // Check that main static files exist
      const staticFiles = [
        'index.html',
        'css/main.css',
        'css/dark-mode.css',
        'css/responsive.css',
        'js/main.js',
        'js/dark-mode.js',
        'js/copy-code.js',
      ];

      for (const file of staticFiles) {
        const filePath = path.join(docsDir, file);
        const exists = fs.existsSync(filePath);
        expect(exists, `Expected ${file} to exist`).toBe(true);
      }

      // Verify no server-side files exist
      const serverSideExtensions = ['.php', '.py', '.rb', '.asp', '.aspx', '.jsp'];
      const checkForServerFiles = (dir: string): boolean => {
        if (!fs.existsSync(dir)) return true;
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
          const entryPath = path.join(dir, entry.name);
          if (entry.isDirectory()) {
            if (!checkForServerFiles(entryPath)) return false;
          } else {
            const ext = path.extname(entry.name).toLowerCase();
            if (serverSideExtensions.includes(ext)) {
              return false;
            }
          }
        }
        return true;
      };

      const noServerFiles = checkForServerFiles(docsDir);
      expect(noServerFiles).toBe(true);
    });

    test('docs directory has valid structure for static hosting', async () => {
      // Verify required directories exist
      const requiredDirs = ['css', 'js', 'assets'];
      for (const dir of requiredDirs) {
        const dirPath = path.join(docsDir, dir);
        const exists = fs.existsSync(dirPath);
        expect(exists, `Expected ${dir}/ directory to exist`).toBe(true);
      }
    });
  });

  test.describe('TC2: No API calls on page load', () => {
    test('page does not make any backend API calls on initial load', async ({ page }) => {
      const apiCalls: string[] = [];
      const blockedPatterns = [
        /\/api\//i,
        /graphql/i,
        /\.json$/i, // JSON API endpoints (excluding CDN/badge services)
      ];

      // Allowed external services (badges, CDNs, fonts)
      const allowedPatterns = [
        /circleci\.com/i,
        /shields\.io/i,
        /fonts\.googleapis\.com/i,
        /fonts\.gstatic\.com/i,
        /cdnjs\.cloudflare\.com/i,
        /unpkg\.com/i,
        /cdn\.jsdelivr\.net/i,
      ];

      // Monitor all network requests
      page.on('request', (request) => {
        const url = request.url();

        // Ignore local requests
        if (url.startsWith('http://localhost') || url.startsWith('file://')) {
          return;
        }

        // Check if it's an allowed external service
        const isAllowed = allowedPatterns.some((pattern) => pattern.test(url));
        if (isAllowed) {
          return;
        }

        // Check for blocked API patterns
        const isApiCall = blockedPatterns.some((pattern) => pattern.test(url));
        if (isApiCall) {
          apiCalls.push(url);
        }
      });

      // Load the page
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify no API calls were made
      expect(apiCalls, `Unexpected API calls found: ${apiCalls.join(', ')}`).toHaveLength(0);
    });

    test('page does not make fetch or XHR calls to backend APIs', async ({ page }) => {
      const fetchCalls: string[] = [];

      // Intercept fetch and XHR requests
      await page.route('**/*', async (route) => {
        const request = route.request();
        const url = request.url();
        const resourceType = request.resourceType();

        // Track fetch/xhr calls that might be API calls
        if (resourceType === 'fetch' || resourceType === 'xhr') {
          // Ignore local requests
          if (!url.startsWith('http://localhost') && !url.startsWith('file://')) {
            // Ignore badge services
            if (!url.includes('circleci.com') && !url.includes('shields.io')) {
              fetchCalls.push(url);
            }
          }
        }

        await route.continue();
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      expect(fetchCalls, `Unexpected fetch/XHR calls: ${fetchCalls.join(', ')}`).toHaveLength(0);
    });

    test('no WebSocket connections are initiated', async ({ page }) => {
      const wsConnections: string[] = [];

      // Monitor WebSocket connections via CDP
      const client = await page.context().newCDPSession(page);
      await client.send('Network.enable');

      client.on('Network.webSocketCreated', (params) => {
        wsConnections.push(params.url);
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      expect(wsConnections, `Unexpected WebSocket connections: ${wsConnections.join(', ')}`).toHaveLength(0);
    });
  });

  test.describe('TC3: Simple HTTP server compatibility', () => {
    test('site functions correctly when served with simple HTTP server', async ({ page }) => {
      // The Playwright config already serves via http-server
      // This test validates the site works in that environment

      // Navigate to the homepage
      const response = await page.goto('/');
      expect(response?.status()).toBe(200);

      // Verify essential elements are present and working
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('h1')).toContainText('MirDB');

      // Verify navigation works
      await expect(page.locator('nav')).toBeVisible();

      // Verify sections are accessible
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#getting-started')).toBeVisible();
    });

    test('all local assets load successfully', async ({ page }) => {
      const failedResources: string[] = [];

      page.on('response', (response) => {
        const url = response.url();
        // Only track local resources
        if (url.startsWith('http://localhost')) {
          if (!response.ok()) {
            failedResources.push(`${url} (${response.status()})`);
          }
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      expect(failedResources, `Failed to load: ${failedResources.join(', ')}`).toHaveLength(0);
    });

    test('CSS files load and apply styles correctly', async ({ page }) => {
      await page.goto('/');

      // Verify CSS is applied by checking computed styles
      const heroTitle = page.locator('h1');
      await expect(heroTitle).toBeVisible();

      // Check that the hero title has styling applied (not browser default)
      const styles = await heroTitle.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          fontFamily: computed.fontFamily,
          fontSize: computed.fontSize,
          color: computed.color,
        };
      });

      // Verify styles are not browser defaults
      expect(styles.fontSize).not.toBe('32px'); // Default h1 size
    });

    test('JavaScript files load and execute without errors', async ({ page }) => {
      const consoleErrors: string[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      page.on('pageerror', (error) => {
        consoleErrors.push(error.message);
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Filter out non-critical errors (like failed badge image loads)
      const criticalErrors = consoleErrors.filter(
        (err) => !err.includes('circleci') && !err.includes('shields.io') && !err.includes('Failed to load resource')
      );

      expect(criticalErrors, `JavaScript errors: ${criticalErrors.join(', ')}`).toHaveLength(0);
    });
  });

  test.describe('TC4: Relative asset paths', () => {
    test('all asset paths in HTML are relative or use CDN URLs', async ({ page }) => {
      await page.goto('/');

      // Check all link elements (CSS)
      const linkHrefs = await page.locator('link[rel="stylesheet"]').evaluateAll((links) =>
        links.map((link) => link.getAttribute('href'))
      );

      for (const href of linkHrefs) {
        if (href) {
          const isRelative = !href.startsWith('http://') && !href.startsWith('https://') && !href.startsWith('//');
          const isCdn =
            href.includes('cdnjs.cloudflare.com') ||
            href.includes('fonts.googleapis.com') ||
            href.includes('unpkg.com') ||
            href.includes('cdn.jsdelivr.net');

          expect(isRelative || isCdn, `CSS href should be relative or CDN: ${href}`).toBe(true);
        }
      }

      // Check all script elements
      const scriptSrcs = await page.locator('script[src]').evaluateAll((scripts) =>
        scripts.map((script) => script.getAttribute('src'))
      );

      for (const src of scriptSrcs) {
        if (src) {
          const isRelative = !src.startsWith('http://') && !src.startsWith('https://') && !src.startsWith('//');
          const isCdn =
            src.includes('cdnjs.cloudflare.com') ||
            src.includes('unpkg.com') ||
            src.includes('cdn.jsdelivr.net');

          expect(isRelative || isCdn, `Script src should be relative or CDN: ${src}`).toBe(true);
        }
      }
    });

    test('all image sources are relative or use allowed CDN/external URLs', async ({ page }) => {
      await page.goto('/');

      // Get all image sources
      const imgSrcs = await page.locator('img').evaluateAll((imgs) =>
        imgs.map((img) => img.getAttribute('src'))
      );

      const allowedExternalPatterns = [
        /circleci\.com/i, // CI badges
        /shields\.io/i, // Status badges
        /githubusercontent\.com/i, // GitHub assets
        /github\.com/i, // GitHub assets
      ];

      for (const src of imgSrcs) {
        if (src) {
          const isRelative = !src.startsWith('http://') && !src.startsWith('https://') && !src.startsWith('//');
          const isAllowedExternal = allowedExternalPatterns.some((pattern) => pattern.test(src));

          expect(
            isRelative || isAllowedExternal,
            `Image src should be relative or allowed external: ${src}`
          ).toBe(true);
        }
      }
    });

    test('all internal links use relative paths or valid anchors', async ({ page }) => {
      await page.goto('/');

      // Get all anchor elements
      const anchorHrefs = await page.locator('a').evaluateAll((anchors) =>
        anchors.map((a) => ({
          href: a.getAttribute('href'),
          text: a.textContent?.trim() || '',
        }))
      );

      for (const { href, text } of anchorHrefs) {
        if (href) {
          // Check if it's an internal link
          const isExternal =
            href.startsWith('http://') || href.startsWith('https://') || href.startsWith('//');
          const isAnchor = href.startsWith('#');
          const isMailto = href.startsWith('mailto:');
          const isRelative = !isExternal && !isAnchor && !isMailto;

          // External links are fine (GitHub, docs, etc.)
          // Internal links should be relative or anchors
          if (!isExternal && !isMailto) {
            expect(
              isRelative || isAnchor,
              `Internal link should be relative or anchor: ${href} (${text})`
            ).toBe(true);
          }
        }
      }
    });

    test('no hardcoded localhost or absolute file paths in HTML', async ({ page }) => {
      await page.goto('/');

      // Get the full HTML content
      const htmlContent = await page.content();

      // Check for problematic patterns
      const problematicPatterns = [
        /href=["']http:\/\/localhost/gi,
        /src=["']http:\/\/localhost/gi,
        /href=["']file:\/\//gi,
        /src=["']file:\/\//gi,
        /href=["']\/[A-Z]:\//gi, // Windows absolute paths
        /src=["']\/[A-Z]:\//gi,
        /href=["']\/home\//gi, // Unix absolute paths
        /src=["']\/home\//gi,
        /href=["']\/Users\//gi, // macOS absolute paths
        /src=["']\/Users\//gi,
      ];

      for (const pattern of problematicPatterns) {
        const matches = htmlContent.match(pattern);
        expect(matches, `Found hardcoded paths: ${matches?.join(', ')}`).toBeNull();
      }
    });

    test('CSS url() references use relative paths', async () => {
      // Read CSS files and check for absolute paths
      const cssFiles = ['main.css', 'dark-mode.css', 'responsive.css'];

      for (const cssFile of cssFiles) {
        const cssPath = path.join(docsDir, 'css', cssFile);
        if (fs.existsSync(cssPath)) {
          const cssContent = fs.readFileSync(cssPath, 'utf-8');

          // Check for absolute URLs in url() declarations
          const absoluteUrlPattern = /url\(\s*["']?(http:\/\/localhost|file:\/\/|\/home\/|\/Users\/|[A-Z]:)/gi;
          const matches = cssContent.match(absoluteUrlPattern);

          expect(matches, `${cssFile} contains absolute paths: ${matches?.join(', ')}`).toBeNull();
        }
      }
    });
  });

  test.describe('Static file validation', () => {
    test('HTML file is valid and well-formed', async () => {
      const indexPath = path.join(docsDir, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check for DOCTYPE
      expect(htmlContent.trim().toLowerCase()).toMatch(/^<!doctype html>/);

      // Check for required elements
      expect(htmlContent).toContain('<html');
      expect(htmlContent).toContain('<head>');
      expect(htmlContent).toContain('</head>');
      expect(htmlContent).toContain('<body>');
      expect(htmlContent).toContain('</body>');
      expect(htmlContent).toContain('</html>');

      // Check for charset and viewport meta tags
      expect(htmlContent).toContain('charset=');
      expect(htmlContent).toContain('viewport');
    });

    test('no server-side code or dependencies in JavaScript files', async () => {
      const jsFiles = ['main.js', 'dark-mode.js', 'copy-code.js'];

      const serverSidePatterns = [
        /require\s*\(\s*['"]fs['"]\s*\)/gi, // Node.js fs
        /require\s*\(\s*['"]http['"]\s*\)/gi, // Node.js http
        /require\s*\(\s*['"]express['"]\s*\)/gi, // Express.js
        /import.*from\s*['"]fs['"]/gi,
        /import.*from\s*['"]http['"]/gi,
        /process\.env\./gi, // Environment variables (server-side)
        /__dirname/gi, // Node.js path
        /__filename/gi, // Node.js path
      ];

      for (const jsFile of jsFiles) {
        const jsPath = path.join(docsDir, 'js', jsFile);
        if (fs.existsSync(jsPath)) {
          const jsContent = fs.readFileSync(jsPath, 'utf-8');

          for (const pattern of serverSidePatterns) {
            const matches = jsContent.match(pattern);
            expect(matches, `${jsFile} contains server-side code: ${matches?.join(', ')}`).toBeNull();
          }
        }
      }
    });
  });
});
