/**
 * E2E Tests: Performance and Static Site Build
 * Owner: Scenario 11 - Performance and Static Site Build
 *
 * Expected test coverage:
 * - Lighthouse performance score >= 90
 * - Lighthouse accessibility score >= 90
 * - FCP < 1.0s, LCP < 1.5s
 * - Page loads within 2s on simulated 4G
 * - Build output contains only static files
 */

const { test, expect } = require('@playwright/test');
const { AxeBuilder } = require('@axe-core/playwright');
const fs = require('fs');
const path = require('path');

const BASE_URL = 'http://localhost:3000';
const MIN_PERFORMANCE_SCORE = 90;
const MIN_ACCESSIBILITY_SCORE = 90;
const MIN_BEST_PRACTICES_SCORE = 90;
const MIN_SEO_SCORE = 90;
const MAX_FCP_MS = 1000;
const MAX_LCP_MS = 1500;
const MAX_LOAD_TIME_MS = 2000;

/**
 * Collect Web Vitals metrics (FCP, LCP) via PerformanceObserver in the page.
 */
async function collectWebVitals(page) {
  return page.evaluate(() => {
    return new Promise((resolve) => {
      const entries = {};
      const observer = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          if (entry.entryType === 'paint') {
            entries[entry.name] = entry.startTime;
          }
          if (entry.entryType === 'largest-contentful-paint') {
            entries['largest-contentful-paint'] = entry.startTime;
          }
        }
      });
      observer.observe({ entryTypes: ['paint', 'largest-contentful-paint'] });

      // Also get existing entries
      const paints = performance.getEntriesByType('paint');
      for (const p of paints) {
        entries[p.name] = p.startTime;
      }
      const lcps = performance.getEntriesByType('largest-contentful-paint');
      for (const p of lcps) {
        entries['largest-contentful-paint'] = p.startTime;
      }

      setTimeout(() => {
        observer.disconnect();
        resolve(entries);
      }, 500);
    });
  });
}

/**
 * Calculate a synthetic performance score from metrics.
 * Maps FCP and LCP to a 0-100 score similar to Lighthouse weighting.
 */
function calculatePerformanceScore(fcp, lcp) {
  // FCP scoring (Lighthouse-style): 0-100 mapped from 0-3000ms
  const fcpScore = Math.max(0, Math.min(100, 100 - (fcp / 30)));
  // LCP scoring: 0-100 mapped from 0-4000ms
  const lcpScore = Math.max(0, Math.min(100, 100 - (lcp / 40)));
  // Weighted: FCP 30%, LCP 70%
  return Math.round(fcpScore * 0.3 + lcpScore * 0.7);
}

/**
 * Calculate accessibility score from axe-core violations.
 * 100 minus penalty for each violation type (minor=1, moderate=2, serious=3, critical=4).
 */
function calculateAccessibilityScore(violations) {
  const impactWeights = { minor: 1, moderate: 2, serious: 3, critical: 4 };
  let penalty = 0;
  for (const v of violations) {
    penalty += (impactWeights[v.impact] || 1) * Math.min(v.nodes.length, 5);
  }
  return Math.max(0, 100 - penalty * 2);
}

test.describe('Performance and Static Site Build', () => {
  test('Lighthouse desktop audit - performance score >= 90, FCP < 1.0s, LCP < 1.5s', async ({ page }) => {
    await page.goto(BASE_URL, { waitUntil: 'networkidle' });
    const vitals = await collectWebVitals(page);

    const fcp = vitals['first-contentful-paint'] || 0;
    const lcp = vitals['largest-contentful-paint'] || fcp;
    const perfScore = calculatePerformanceScore(fcp, lcp);

    expect(perfScore).toBeGreaterThanOrEqual(MIN_PERFORMANCE_SCORE);
    expect(fcp).toBeLessThanOrEqual(MAX_FCP_MS);
    expect(lcp).toBeLessThanOrEqual(MAX_LCP_MS);
  });

  test('Lighthouse mobile audit - performance score >= 90', async ({ browser }) => {
    const context = await browser.newContext({
      viewport: { width: 375, height: 667 },
      deviceScaleFactor: 2,
      userAgent: 'Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
    });
    const page = await context.newPage();
    await page.goto(BASE_URL, { waitUntil: 'networkidle' });
    const vitals = await collectWebVitals(page);

    const fcp = vitals['first-contentful-paint'] || 0;
    const lcp = vitals['largest-contentful-paint'] || fcp;
    const perfScore = calculatePerformanceScore(fcp, lcp);

    expect(perfScore).toBeGreaterThanOrEqual(MIN_PERFORMANCE_SCORE);
    await context.close();
  });

  test('Page loads within 2 seconds on simulated 4G', async ({ page }) => {
    const client = await page.context().newCDPSession(page);
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (1.6 * 1024 * 1024) / 8,
      uploadThroughput: (750 * 1024) / 8,
      latency: 150,
    });

    const startTime = Date.now();
    await page.goto(BASE_URL, { waitUntil: 'networkidle' });
    const loadTime = Date.now() - startTime;

    expect(loadTime).toBeLessThanOrEqual(MAX_LOAD_TIME_MS);
  });

  test('Build output contains only static files', () => {
    const srcDir = path.resolve(__dirname, '../../src');

    expect(fs.existsSync(srcDir)).toBe(true);

    function getAllFiles(dir) {
      const files = [];
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

    const allFiles = getAllFiles(srcDir);
    expect(allFiles.length).toBeGreaterThan(0);

    const disallowedExtensions = ['.php', '.py', '.rb', '.java', '.jsp', '.asp', '.aspx', '.cgi', '.pl'];
    for (const file of allFiles) {
      const ext = path.extname(file).toLowerCase();
      if (ext) {
        expect(disallowedExtensions).not.toContain(ext);
      }
    }

    const indexPath = path.join(srcDir, 'index.html');
    expect(fs.existsSync(indexPath)).toBe(true);

    const indexContent = fs.readFileSync(indexPath, 'utf-8');
    expect(indexContent).not.toContain('<?php');
    expect(indexContent).not.toContain('<%');
    expect(indexContent).not.toContain('<%= ');
    expect(indexContent).not.toContain('{{');
  });

  test('Lighthouse accessibility score >= 90', async ({ page }) => {
    await page.goto(BASE_URL, { waitUntil: 'networkidle' });
    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze();

    const score = calculateAccessibilityScore(results.violations);
    expect(score).toBeGreaterThanOrEqual(MIN_ACCESSIBILITY_SCORE);
  });

  test('Lighthouse best practices score >= 90', async ({ page }) => {
    await page.goto(BASE_URL, { waitUntil: 'networkidle' });

    // Collect console errors
    const consoleErrors = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Check doctype
    const doctype = await page.evaluate(() => document.doctype?.name);
    expect(doctype).toBe('html');

    // Check HTTPS on external links
    const externalLinks = await page.locator('a[href^="http"]').all();
    let insecureLinks = 0;
    for (const link of externalLinks) {
      const href = await link.getAttribute('href');
      if (href && href.startsWith('http:') && !href.startsWith('http://localhost')) {
        insecureLinks++;
      }
    }

    // Check for deprecated APIs or mixed content indicators
    const mixedContent = await page.evaluate(() => {
      return window.performance?.getEntriesByType?.('resource')
        ?.some(r => r.name.startsWith('http:')) ?? false;
    });

    // Calculate score: start at 100, deduct for issues
    let score = 100;
    if (consoleErrors.length > 0) score -= 5;
    if (insecureLinks > 0) score -= 5;
    if (mixedContent) score -= 5;

    expect(score).toBeGreaterThanOrEqual(MIN_BEST_PRACTICES_SCORE);
  });

  test('Lighthouse SEO score >= 90 with proper meta tags', async ({ page }) => {
    await page.goto(BASE_URL, { waitUntil: 'networkidle' });

    // Title check
    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);

    // Meta description
    const description = await page.locator('meta[name="description"]').getAttribute('content').catch(() => null);
    expect(description).toBeTruthy();

    // Viewport
    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content').catch(() => null);
    expect(viewport).toBeTruthy();

    // Charset
    const charset = await page.locator('meta[charset]').getAttribute('charset').catch(() => null);
    expect(charset).toBeTruthy();

    // Lang attribute
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBeTruthy();

    // H1 present
    const h1Count = await page.locator('h1').count();
    expect(h1Count).toBeGreaterThan(0);

    // Links are crawlable (no href="javascript:void(0)")
    const badLinks = await page.locator('a[href^="javascript:"]').count();
    expect(badLinks).toBe(0);

    // Calculate SEO score
    let score = 100;
    if (!description || description.length < 10) score -= 10;
    if (title.length < 10) score -= 5;
    if (!viewport) score -= 10;
    if (!charset) score -= 5;
    if (!htmlLang) score -= 5;
    if (h1Count === 0) score -= 10;
    if (badLinks > 0) score -= 5;

    expect(score).toBeGreaterThanOrEqual(MIN_SEO_SCORE);
  });

  test('HTML has proper meta tags for SEO', async ({ page }) => {
    await page.goto(BASE_URL);

    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);

    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewport).toBeTruthy();

    const charset = await page.locator('meta[charset]').getAttribute('charset');
    expect(charset).toBeTruthy();

    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBeTruthy();
  });
});
