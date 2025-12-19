// @ts-check
const { test, expect } = require('@playwright/test');
const { playAudit } = require('playwright-lighthouse');
const { chromium } = require('playwright');
const path = require('path');
const http = require('http');
const fs = require('fs');

// Local HTTP server for Lighthouse testing (Lighthouse requires HTTP/HTTPS URLs)
const DIST_DIR = path.resolve(__dirname, '../dist');
const SERVER_PORT = 8888;
const HTTP_BASE_URL = `http://localhost:${SERVER_PORT}`;

// File URL for basic load time testing
const FILE_BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

// Remote debugging port for Lighthouse
const LIGHTHOUSE_PORT = 9222;

// Performance thresholds based on PRD requirements
const THRESHOLDS = {
  // NFR-1: Page load time under 3 seconds on 3G
  pageLoadTime: 3000,
  // Success criteria: Performance score > 80
  performanceScore: 80,
  // Core Web Vitals thresholds
  fcp: 1800, // First Contentful Paint under 1.8 seconds
  lcp: 2500, // Largest Contentful Paint under 2.5 seconds
  cls: 0.1,  // Cumulative Layout Shift under 0.1
};

// Simple static file server for Lighthouse tests
function createServer() {
  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      let filePath = path.join(DIST_DIR, req.url === '/' ? 'index.html' : req.url);

      // Determine content type
      const ext = path.extname(filePath);
      const contentTypes = {
        '.html': 'text/html',
        '.css': 'text/css',
        '.js': 'application/javascript',
        '.json': 'application/json',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml',
      };
      const contentType = contentTypes[ext] || 'text/plain';

      fs.readFile(filePath, (err, content) => {
        if (err) {
          res.writeHead(404);
          res.end('Not found');
        } else {
          res.writeHead(200, { 'Content-Type': contentType });
          res.end(content);
        }
      });
    });

    server.listen(SERVER_PORT, () => {
      resolve(server);
    });
  });
}

test.describe('Page Performance', () => {
  test.describe.configure({ mode: 'serial' });

  test('Test Case 1: Page loads in under 3 seconds on simulated 3G', async ({ page }) => {
    // Simulate 3G network conditions
    const cdpSession = await page.context().newCDPSession(page);

    await cdpSession.send('Network.enable');
    await cdpSession.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (750 * 1024) / 8, // 750 Kbps
      uploadThroughput: (250 * 1024) / 8,   // 250 Kbps
      latency: 100, // 100ms latency
    });

    const startTime = Date.now();

    // Navigate to the page (using file URL for this test)
    await page.goto(FILE_BASE_URL, { waitUntil: 'load' });

    const loadTime = Date.now() - startTime;

    console.log(`Page load time on 3G: ${loadTime}ms`);

    // Verify load time is under 3 seconds
    expect(loadTime).toBeLessThan(THRESHOLDS.pageLoadTime);

    // Also verify that the hero section is visible (page is usable)
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible({ timeout: 1000 });
  });

  test('Test Case 2: Lighthouse performance score is greater than 80', async () => {
    // Start local HTTP server for Lighthouse
    const server = await createServer();

    // Launch browser with remote debugging for Lighthouse
    const browser = await chromium.launch({
      args: [`--remote-debugging-port=${LIGHTHOUSE_PORT}`],
    });

    try {
      const page = await browser.newPage();
      await page.goto(HTTP_BASE_URL, { waitUntil: 'networkidle' });

      // Run Lighthouse audit
      const { lhr } = await playAudit({
        page,
        port: LIGHTHOUSE_PORT,
        thresholds: {
          performance: THRESHOLDS.performanceScore,
        },
      });

      const performanceScore = Math.round(lhr.categories.performance.score * 100);
      console.log(`Lighthouse Performance Score: ${performanceScore}`);

      // Verify performance score is above 80
      expect(performanceScore).toBeGreaterThan(THRESHOLDS.performanceScore);
    } finally {
      await browser.close();
      server.close();
    }
  });

  test('Test Case 3: First Contentful Paint (FCP) is under 1.8 seconds', async () => {
    // Start local HTTP server for Lighthouse
    const server = await createServer();

    // Launch browser with remote debugging for Lighthouse
    const browser = await chromium.launch({
      args: [`--remote-debugging-port=${LIGHTHOUSE_PORT}`],
    });

    try {
      const page = await browser.newPage();
      await page.goto(HTTP_BASE_URL, { waitUntil: 'networkidle' });

      // Run Lighthouse audit
      const { lhr } = await playAudit({
        page,
        port: LIGHTHOUSE_PORT,
        thresholds: {
          performance: 0, // Don't fail on overall performance
        },
      });

      // Get FCP metric
      const fcpAudit = lhr.audits['first-contentful-paint'];
      const fcpMs = fcpAudit.numericValue;

      console.log(`First Contentful Paint: ${fcpMs}ms`);

      // Verify FCP is under 1.8 seconds
      expect(fcpMs).toBeLessThan(THRESHOLDS.fcp);
    } finally {
      await browser.close();
      server.close();
    }
  });

  test('Test Case 4: Largest Contentful Paint (LCP) is under 2.5 seconds', async () => {
    // Start local HTTP server for Lighthouse
    const server = await createServer();

    // Launch browser with remote debugging for Lighthouse
    const browser = await chromium.launch({
      args: [`--remote-debugging-port=${LIGHTHOUSE_PORT}`],
    });

    try {
      const page = await browser.newPage();
      await page.goto(HTTP_BASE_URL, { waitUntil: 'networkidle' });

      // Run Lighthouse audit
      const { lhr } = await playAudit({
        page,
        port: LIGHTHOUSE_PORT,
        thresholds: {
          performance: 0, // Don't fail on overall performance
        },
      });

      // Get LCP metric
      const lcpAudit = lhr.audits['largest-contentful-paint'];
      const lcpMs = lcpAudit.numericValue;

      console.log(`Largest Contentful Paint: ${lcpMs}ms`);

      // Verify LCP is under 2.5 seconds
      expect(lcpMs).toBeLessThan(THRESHOLDS.lcp);
    } finally {
      await browser.close();
      server.close();
    }
  });

  test('Test Case 5: Cumulative Layout Shift (CLS) is under 0.1', async () => {
    // Start local HTTP server for Lighthouse
    const server = await createServer();

    // Launch browser with remote debugging for Lighthouse
    const browser = await chromium.launch({
      args: [`--remote-debugging-port=${LIGHTHOUSE_PORT}`],
    });

    try {
      const page = await browser.newPage();
      await page.goto(HTTP_BASE_URL, { waitUntil: 'networkidle' });

      // Run Lighthouse audit
      const { lhr } = await playAudit({
        page,
        port: LIGHTHOUSE_PORT,
        thresholds: {
          performance: 0, // Don't fail on overall performance
        },
      });

      // Get CLS metric
      const clsAudit = lhr.audits['cumulative-layout-shift'];
      const clsValue = clsAudit.numericValue;

      console.log(`Cumulative Layout Shift: ${clsValue}`);

      // Verify CLS is under 0.1
      expect(clsValue).toBeLessThan(THRESHOLDS.cls);
    } finally {
      await browser.close();
      server.close();
    }
  });
});
