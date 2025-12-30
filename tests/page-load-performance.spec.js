// @ts-check
const { test, expect } = require('@playwright/test');
const lighthouseModule = require('lighthouse');
const lighthouse = lighthouseModule.default;
const puppeteer = require('puppeteer');
const path = require('path');
const fs = require('fs');
const http = require('http');

/**
 * Page Load Performance Test Suite
 *
 * Tests the homepage performance against the following criteria:
 * 1. Page fully loads in under 2 seconds on broadband connection
 * 2. Lighthouse performance score is 80 or above
 * 3. First Contentful Paint (FCP) is under 1.8 seconds
 * 4. Total page size (HTML, CSS, images) is optimized and reasonable
 */

const HOMEPAGE_PATH = path.resolve(__dirname, '..', 'index.html');
const HOMEPAGE_URL = `file://${HOMEPAGE_PATH}`;
const PROJECT_DIR = path.resolve(__dirname, '..');

// Performance thresholds based on PRD requirements
const PERFORMANCE_THRESHOLDS = {
  maxPageLoadTime: 2000, // 2 seconds in milliseconds
  minLighthouseScore: 80, // minimum Lighthouse performance score
  maxFCP: 1800, // 1.8 seconds in milliseconds for First Contentful Paint
  maxPageWeight: 500 * 1024, // 500 KB max total page weight (reasonable for static site)
};

/**
 * Creates a simple HTTP server for serving static files
 * Lighthouse requires HTTP/HTTPS URLs (not file://)
 */
function createServer(port = 0) {
  const mimeTypes = {
    '.html': 'text/html',
    '.css': 'text/css',
    '.js': 'application/javascript',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.gif': 'image/gif',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon',
  };

  const server = http.createServer((req, res) => {
    let filePath = req.url === '/' ? '/index.html' : req.url;
    filePath = path.join(PROJECT_DIR, filePath);

    const ext = path.extname(filePath);
    const contentType = mimeTypes[ext] || 'application/octet-stream';

    fs.readFile(filePath, (err, content) => {
      if (err) {
        if (err.code === 'ENOENT') {
          res.writeHead(404);
          res.end('Not Found');
        } else {
          res.writeHead(500);
          res.end('Server Error');
        }
      } else {
        res.writeHead(200, { 'Content-Type': contentType });
        res.end(content);
      }
    });
  });

  return new Promise((resolve) => {
    server.listen(port, () => {
      const address = server.address();
      const actualPort = typeof address === 'object' ? address.port : port;
      resolve({ server, port: actualPort, url: `http://localhost:${actualPort}` });
    });
  });
}

test.describe('Page Load Performance', () => {

  test('TC1: Page fully loads in under 2 seconds', async ({ page }) => {
    // Clear any cached resources by creating a fresh context
    const startTime = Date.now();

    // Navigate to the page and wait for network idle
    await page.goto(HOMEPAGE_URL, { waitUntil: 'networkidle' });

    const loadTime = Date.now() - startTime;

    // Verify the page loaded successfully
    await expect(page.locator('header.hero')).toBeVisible();
    await expect(page.locator('footer.footer')).toBeVisible();

    // Assert page load time is under 2 seconds
    expect(loadTime).toBeLessThan(PERFORMANCE_THRESHOLDS.maxPageLoadTime);

    console.log(`Page load time: ${loadTime}ms (threshold: ${PERFORMANCE_THRESHOLDS.maxPageLoadTime}ms)`);
  });

  test('TC2: Lighthouse performance score is 80 or above', async () => {
    // Create a local HTTP server for Lighthouse (it requires HTTP URLs)
    const { server, url } = await createServer();

    // Launch Puppeteer browser for Lighthouse testing
    const browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });

    try {
      // Run Lighthouse audit
      const { lhr } = await lighthouse(url, {
        port: new URL(browser.wsEndpoint()).port,
        output: 'json',
        logLevel: 'error',
        onlyCategories: ['performance'],
      });

      const performanceScore = Math.round(lhr.categories.performance.score * 100);

      console.log(`Lighthouse performance score: ${performanceScore} (threshold: ${PERFORMANCE_THRESHOLDS.minLighthouseScore})`);

      // Assert performance score meets the minimum threshold
      expect(performanceScore).toBeGreaterThanOrEqual(PERFORMANCE_THRESHOLDS.minLighthouseScore);

    } finally {
      await browser.close();
      server.close();
    }
  });

  test('TC3: First Contentful Paint is under 1.8 seconds', async () => {
    // Create a local HTTP server for Lighthouse
    const { server, url } = await createServer();

    // Launch Puppeteer browser for Lighthouse testing
    const browser = await puppeteer.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });

    try {
      // Run Lighthouse audit focusing on FCP
      const { lhr } = await lighthouse(url, {
        port: new URL(browser.wsEndpoint()).port,
        output: 'json',
        logLevel: 'error',
        onlyCategories: ['performance'],
      });

      // Get FCP metric from Lighthouse results
      const fcpAudit = lhr.audits['first-contentful-paint'];
      const fcpValue = fcpAudit.numericValue; // in milliseconds

      console.log(`First Contentful Paint: ${Math.round(fcpValue)}ms (threshold: ${PERFORMANCE_THRESHOLDS.maxFCP}ms)`);

      // Assert FCP is under threshold
      expect(fcpValue).toBeLessThan(PERFORMANCE_THRESHOLDS.maxFCP);

    } finally {
      await browser.close();
      server.close();
    }
  });

  test('TC4: Total page size is optimized and reasonable', async ({ page }) => {
    // Track all resources loaded
    const resources = [];

    page.on('response', async (response) => {
      try {
        const buffer = await response.body();
        resources.push({
          url: response.url(),
          size: buffer.length,
          type: response.headers()['content-type'] || 'unknown',
        });
      } catch (e) {
        // Some responses may not have a body
      }
    });

    await page.goto(HOMEPAGE_URL, { waitUntil: 'networkidle' });

    // Calculate total page weight
    const totalSize = resources.reduce((sum, r) => sum + r.size, 0);

    // Calculate sizes by type
    const htmlSize = fs.statSync(HOMEPAGE_PATH).size;
    const cssPath = path.resolve(__dirname, '..', 'styles.css');
    const cssSize = fs.statSync(cssPath).size;

    console.log(`Page weight breakdown:`);
    console.log(`  HTML: ${(htmlSize / 1024).toFixed(2)} KB`);
    console.log(`  CSS: ${(cssSize / 1024).toFixed(2)} KB`);
    console.log(`  Total loaded resources: ${(totalSize / 1024).toFixed(2)} KB`);
    console.log(`  Threshold: ${(PERFORMANCE_THRESHOLDS.maxPageWeight / 1024).toFixed(2)} KB`);

    // Assert total page weight is under threshold
    expect(totalSize).toBeLessThan(PERFORMANCE_THRESHOLDS.maxPageWeight);

    // Additional check: HTML + CSS should be reasonably small
    const coreSize = htmlSize + cssSize;
    expect(coreSize).toBeLessThan(100 * 1024); // Core files should be under 100KB

    console.log(`Core files (HTML + CSS): ${(coreSize / 1024).toFixed(2)} KB`);
  });

  test('Page has all critical sections visible on load', async ({ page }) => {
    // This test ensures the page is fully functional
    await page.goto(HOMEPAGE_URL, { waitUntil: 'domcontentloaded' });

    // Verify critical sections are present and visible
    await expect(page.locator('header.hero')).toBeVisible();
    await expect(page.locator('.hero-title')).toBeVisible();
    await expect(page.locator('.hero-tagline')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quick-start')).toBeVisible();
    await expect(page.locator('#architecture')).toBeVisible();
    await expect(page.locator('#commands')).toBeVisible();
    await expect(page.locator('footer.footer')).toBeVisible();
  });

  test('Page content is accessible without JavaScript', async ({ browser }) => {
    // Create a new browser context with JavaScript disabled
    const context = await browser.newContext({
      javaScriptEnabled: false
    });
    const page = await context.newPage();

    await page.goto(HOMEPAGE_URL, { waitUntil: 'domcontentloaded' });

    // Verify all content is still visible without JS
    await expect(page.locator('.hero-title')).toHaveText('MirDB');
    await expect(page.locator('.hero-tagline')).toContainText('Persistent Key-Value Store');
    await expect(page.locator('#features .feature-card')).toHaveCount(3);
    await expect(page.locator('.code-block')).toHaveCount(3);

    await context.close();
  });

  test('Performance metrics using Navigation Timing API', async ({ page }) => {
    await page.goto(HOMEPAGE_URL, { waitUntil: 'networkidle' });

    // Get performance timing data
    const performanceTiming = await page.evaluate(() => {
      const timing = performance.timing;
      return {
        domContentLoaded: timing.domContentLoadedEventEnd - timing.navigationStart,
        loadComplete: timing.loadEventEnd - timing.navigationStart,
        domInteractive: timing.domInteractive - timing.navigationStart,
        firstPaint: performance.getEntriesByType('paint').find(e => e.name === 'first-paint')?.startTime || 0,
        firstContentfulPaint: performance.getEntriesByType('paint').find(e => e.name === 'first-contentful-paint')?.startTime || 0,
      };
    });

    console.log(`Navigation Timing API metrics:`);
    console.log(`  DOM Interactive: ${performanceTiming.domInteractive}ms`);
    console.log(`  DOM Content Loaded: ${performanceTiming.domContentLoaded}ms`);
    console.log(`  Load Complete: ${performanceTiming.loadComplete}ms`);
    console.log(`  First Paint: ${Math.round(performanceTiming.firstPaint)}ms`);
    console.log(`  First Contentful Paint: ${Math.round(performanceTiming.firstContentfulPaint)}ms`);

    // Assert DOM is interactive quickly
    expect(performanceTiming.domInteractive).toBeLessThan(1000);

    // Assert load complete is under threshold
    expect(performanceTiming.loadComplete).toBeLessThan(PERFORMANCE_THRESHOLDS.maxPageLoadTime);
  });
});
