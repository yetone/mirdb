import { test, expect } from '@playwright/test';
import lighthouse from 'lighthouse';
import * as chromeLauncher from 'chrome-launcher';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Helper to run Lighthouse audit
test.describe('Lighthouse Performance Tests', () => {
  const PORT = 4321;
  let server;

  test.beforeAll(async () => {
    // Serve the dist directory
    const handler = async (req, res) => {
      const fs = await import('fs');
      const path = await import('path');
      const http = await import('http');
      const url = new URL(req.url, `http://localhost:${PORT}`);
      let pathname = url.pathname;

      if (pathname === '/') pathname = '/index.html';

      const filePath = path.join(__dirname, '..', 'dist', pathname);

      try {
        const content = fs.readFileSync(filePath);
        const ext = path.extname(filePath);
        const contentTypes = {
          '.html': 'text/html',
          '.css': 'text/css',
          '.js': 'text/javascript',
          '.json': 'application/json',
          '.png': 'image/png',
          '.jpg': 'image/jpeg',
          '.gif': 'image/gif',
          '.svg': 'image/svg+xml',
        };

        res.writeHead(200, { 'Content-Type': contentTypes[ext] || 'text/plain' });
        res.end(content);
      } catch (err) {
        res.writeHead(404);
        res.end('Not found');
      }
    };

    const http = await import('http');
    server = http.createServer(handler);
    await new Promise(resolve => server.listen(PORT, resolve));
    console.log(`Test server running on http://localhost:${PORT}`);
  });

  test.afterAll(async () => {
    if (server) {
      server.close();
    }
  });

  test('Lighthouse performance score should be above 90', async () => {
    const chrome = await chromeLauncher.launch({ chromeFlags: ['--headless'] });
    const options = {
      logLevel: 'info',
      output: 'json',
      port: chrome.port,
    };

    try {
      const runnerResult = await lighthouse(`http://localhost:${PORT}`, options);

      const categories = runnerResult.lhr.categories;
      const performanceScore = categories.performance.score * 100;
      const accessibilityScore = categories.accessibility.score * 100;
      const bestPracticesScore = categories['best-practices'].score * 100;
      const seoScore = categories.seo.score * 100;

      console.log('\n=== Lighthouse Results ===');
      console.log(`Performance: ${performanceScore.toFixed(0)}`);
      console.log(`Accessibility: ${accessibilityScore.toFixed(0)}`);
      console.log(`Best Practices: ${bestPracticesScore.toFixed(0)}`);
      console.log(`SEO: ${seoScore.toFixed(0)}`);

      // Core Web Vitals
      const metrics = runnerResult.lhr.audits;
      console.log('\n=== Core Web Vitals ===');
      console.log(`First Contentful Paint: ${metrics['first-contentful-paint'].displayValue}`);
      console.log(`Largest Contentful Paint: ${metrics['largest-contentful-paint'].displayValue}`);
      console.log(`Speed Index: ${metrics['speed-index'].displayValue}`);
      console.log(`Time to Interactive: ${metrics['interactive'].displayValue}`);

      expect(performanceScore).toBeGreaterThanOrEqual(90);
      expect(accessibilityScore).toBeGreaterThanOrEqual(90);
      expect(bestPracticesScore).toBeGreaterThanOrEqual(90);
      expect(seoScore).toBeGreaterThanOrEqual(90);
    } finally {
      await chrome.kill();
    }
  });

  test('First Contentful Paint should be under 3 seconds on 3G', async () => {
    const chrome = await chromeLauncher.launch({ chromeFlags: ['--headless'] });
    const options = {
      logLevel: 'error',
      output: 'json',
      port: chrome.port,
      throttling: 'devtools', // Simulate 3G
    };

    try {
      const runnerResult = await lighthouse(`http://localhost:${PORT}`, options);
      const metrics = runnerResult.lhr.audits;

      const fcp = metrics['first-contentful-paint'].numericValue; // in ms
      const lcp = metrics['largest-contentful-paint'].numericValue; // in ms

      console.log(`\nFirst Contentful Paint: ${(fcp / 1000).toFixed(2)}s`);
      console.log(`Largest Contentful Paint: ${(lcp / 1000).toFixed(2)}s`);

      // Should load within 3 seconds as per requirement
      expect(fcp).toBeLessThan(3000);
      expect(lcp).toBeLessThan(3000);
    } finally {
      await chrome.kill();
    }
  });
});
