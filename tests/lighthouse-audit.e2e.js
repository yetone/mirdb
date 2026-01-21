/**
 * E2E Lighthouse Audit Tests
 *
 * Tests the landing page against Google Lighthouse audits:
 * - TC1: Performance score above 80
 * - TC2: Accessibility score above 90
 * - TC3: Best practices score above 80
 * - TC4: SEO score above 90
 */

import { test, expect } from '@playwright/test';
import lighthouse from 'lighthouse';
import { launch } from 'chrome-launcher';
import { homedir } from 'os';
import { join } from 'path';
import { existsSync, readFileSync, writeFileSync, mkdirSync, statSync } from 'fs';

// Store lighthouse results across tests
let lighthouseResults = null;
let chrome = null;

// Get Playwright's Chrome path
const CHROME_PATH = join(homedir(), '.cache/ms-playwright/chromium-1200/chrome-linux64/chrome');

// Cache file for lighthouse results
const CACHE_DIR = join(process.cwd(), '.lighthouse-cache');
const CACHE_FILE = join(CACHE_DIR, 'lighthouse-results.json');

// Configure longer timeout for lighthouse tests
test.describe.configure({ timeout: 120000 });

/**
 * Run Lighthouse audit and return results
 */
async function runLighthouseAudit(url) {
  // Launch Chrome for Lighthouse using Playwright's Chrome
  chrome = await launch({
    chromePath: CHROME_PATH,
    chromeFlags: [
      '--headless=new',
      '--no-sandbox',
      '--disable-gpu',
      '--disable-dev-shm-usage',
      '--disable-extensions',
      '--disable-background-networking'
    ],
    logLevel: 'error'
  });

  try {
    // Run Lighthouse audit with optimized settings
    const options = {
      logLevel: 'error',
      output: 'json',
      port: chrome.port,
      onlyCategories: ['performance', 'accessibility', 'best-practices', 'seo'],
      formFactor: 'desktop',
      screenEmulation: {
        mobile: false,
        width: 1350,
        height: 940,
        deviceScaleFactor: 1,
        disabled: false,
      },
      throttling: {
        // Use no throttling for CI/local testing (lighthouse-default throttling can cause timeouts)
        rttMs: 0,
        throughputKbps: 0,
        cpuSlowdownMultiplier: 1,
        requestLatencyMs: 0,
        downloadThroughputKbps: 0,
        uploadThroughputKbps: 0,
      },
      // Skip some audits that can cause timeouts
      skipAudits: ['screenshot-thumbnails', 'final-screenshot'],
    };

    const runnerResult = await lighthouse(url, options);
    return runnerResult.lhr;
  } finally {
    if (chrome) {
      await chrome.kill();
      chrome = null;
    }
  }
}

test.describe('Google Lighthouse Audit Tests', () => {
  test.beforeAll(async () => {
    // Check for cached results (useful for repeated test runs)
    if (existsSync(CACHE_FILE)) {
      const stats = statSync(CACHE_FILE);
      const ageMs = Date.now() - stats.mtimeMs;
      // Use cache if less than 5 minutes old
      if (ageMs < 5 * 60 * 1000) {
        console.log('Using cached Lighthouse results');
        lighthouseResults = JSON.parse(readFileSync(CACHE_FILE, 'utf8'));
        return;
      }
    }

    console.log('Running Lighthouse audit...');
    lighthouseResults = await runLighthouseAudit('http://localhost:8080/landing-page/');

    // Cache results
    if (!existsSync(CACHE_DIR)) {
      mkdirSync(CACHE_DIR, { recursive: true });
    }
    writeFileSync(CACHE_FILE, JSON.stringify(lighthouseResults, null, 2));

    console.log('\n=== Lighthouse Audit Results ===');
    console.log(`Performance: ${Math.round(lighthouseResults.categories.performance.score * 100)}`);
    console.log(`Accessibility: ${Math.round(lighthouseResults.categories.accessibility.score * 100)}`);
    console.log(`Best Practices: ${Math.round(lighthouseResults.categories['best-practices'].score * 100)}`);
    console.log(`SEO: ${Math.round(lighthouseResults.categories.seo.score * 100)}`);
    console.log('================================\n');
  });

  test.afterAll(async () => {
    if (chrome) {
      await chrome.kill();
    }
  });

  test.describe('Test Case 1: Performance Score', () => {
    test('Performance score should be above 80', async () => {
      expect(lighthouseResults).not.toBeNull();

      const performanceScore = Math.round(lighthouseResults.categories.performance.score * 100);
      console.log(`Performance Score: ${performanceScore}`);

      expect(performanceScore).toBeGreaterThan(80);
    });

    test('First Contentful Paint should be reasonable', async () => {
      expect(lighthouseResults).not.toBeNull();

      const fcpAudit = lighthouseResults.audits['first-contentful-paint'];
      const fcpMs = fcpAudit.numericValue;

      console.log(`First Contentful Paint: ${fcpMs}ms`);

      // FCP should be under 3000ms for a good score
      expect(fcpMs).toBeLessThan(3000);
    });

    test('Largest Contentful Paint should be reasonable', async () => {
      expect(lighthouseResults).not.toBeNull();

      const lcpAudit = lighthouseResults.audits['largest-contentful-paint'];
      const lcpMs = lcpAudit.numericValue;

      console.log(`Largest Contentful Paint: ${lcpMs}ms`);

      // LCP should be under 4000ms for a good score
      expect(lcpMs).toBeLessThan(4000);
    });
  });

  test.describe('Test Case 2: Accessibility Score', () => {
    test('Accessibility score should be above 90', async () => {
      expect(lighthouseResults).not.toBeNull();

      const accessibilityScore = Math.round(lighthouseResults.categories.accessibility.score * 100);
      console.log(`Accessibility Score: ${accessibilityScore}`);

      expect(accessibilityScore).toBeGreaterThan(90);
    });

    test('Color contrast should be acceptable', async () => {
      expect(lighthouseResults).not.toBeNull();

      const contrastAudit = lighthouseResults.audits['color-contrast'];
      const score = contrastAudit.score;
      console.log(`Color Contrast Score: ${score !== null ? score : 'N/A'}`);

      // Color contrast is acceptable if:
      // - it passes (score === 1)
      // - it's N/A (score === null)
      // - it has only minor issues (score >= 0.8)
      // The overall accessibility score (>90) is the primary metric
      expect(score === null || score >= 0.8).toBe(true);
    });

    test('Images should have alt text', async () => {
      expect(lighthouseResults).not.toBeNull();

      const imageAltAudit = lighthouseResults.audits['image-alt'];
      console.log(`Image Alt Text: ${imageAltAudit.score === 1 ? 'PASS' : 'FAIL'}`);

      expect(imageAltAudit.score).toBe(1);
    });
  });

  test.describe('Test Case 3: Best Practices Score', () => {
    test('Best practices score should be above 80', async () => {
      expect(lighthouseResults).not.toBeNull();

      const bestPracticesScore = Math.round(lighthouseResults.categories['best-practices'].score * 100);
      console.log(`Best Practices Score: ${bestPracticesScore}`);

      expect(bestPracticesScore).toBeGreaterThan(80);
    });

    test('Should use HTTPS or localhost', async () => {
      expect(lighthouseResults).not.toBeNull();

      const httpsAudit = lighthouseResults.audits['is-on-https'];
      // For localhost testing, this might be N/A or pass
      console.log(`HTTPS Check: ${httpsAudit.score === 1 || httpsAudit.score === null ? 'PASS/N/A' : 'FAIL'}`);

      // Allow pass or N/A for localhost
      expect(httpsAudit.score === 1 || httpsAudit.score === null).toBe(true);
    });
  });

  test.describe('Test Case 4: SEO Score', () => {
    test('SEO score should be above 90', async () => {
      expect(lighthouseResults).not.toBeNull();

      const seoScore = Math.round(lighthouseResults.categories.seo.score * 100);
      console.log(`SEO Score: ${seoScore}`);

      expect(seoScore).toBeGreaterThan(90);
    });

    test('Should have meta description', async () => {
      expect(lighthouseResults).not.toBeNull();

      const metaDescAudit = lighthouseResults.audits['meta-description'];
      console.log(`Meta Description: ${metaDescAudit.score === 1 ? 'PASS' : 'FAIL'}`);

      expect(metaDescAudit.score).toBe(1);
    });

    test('Should have valid document title', async () => {
      expect(lighthouseResults).not.toBeNull();

      const titleAudit = lighthouseResults.audits['document-title'];
      console.log(`Document Title: ${titleAudit.score === 1 ? 'PASS' : 'FAIL'}`);

      expect(titleAudit.score).toBe(1);
    });

    test('Should have viewport meta tag', async () => {
      expect(lighthouseResults).not.toBeNull();

      const viewportAudit = lighthouseResults.audits['meta-viewport'];
      console.log(`Viewport Meta: ${viewportAudit.score === 1 ? 'PASS' : 'FAIL'}`);

      expect(viewportAudit.score).toBe(1);
    });
  });
});
