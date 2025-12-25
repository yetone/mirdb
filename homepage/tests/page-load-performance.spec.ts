import { test, expect, chromium } from '@playwright/test';
import { playAudit } from 'playwright-lighthouse';

/**
 * Page Load Performance Tests
 *
 * These tests verify that the MirDB homepage meets performance requirements:
 * - NFR-1: Page must load within 3 seconds on standard connections
 * - Lighthouse performance score should be above 90
 * - Core Web Vitals thresholds for FCP, LCP, and CLS
 */

// Lighthouse tests require a specific port for remote debugging
const LIGHTHOUSE_PORT = 9222;

// Performance thresholds based on PRD requirements
const PERFORMANCE_THRESHOLDS = {
  performance: 90,         // Lighthouse performance score >= 90
  accessibility: 80,       // Accessibility baseline
  'best-practices': 80,    // Best practices baseline
  seo: 80,                 // SEO baseline
};

test.describe('Page Load Performance', () => {

  test('TC1: Page becomes interactive within 3 seconds on standard connection', async ({ page }) => {
    // Measure page load time
    const startTime = Date.now();

    // Navigate to homepage
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Wait for the page to become interactive (hero section visible)
    await page.locator('[data-testid="hero-section"]').waitFor({ state: 'visible' });

    const loadTime = Date.now() - startTime;

    // Verify page loads within 3 seconds (3000ms) per NFR-1
    expect(loadTime).toBeLessThan(3000);

    console.log(`Page load time: ${loadTime}ms`);
  });

  test('TC2: Lighthouse performance score is 90 or above', async () => {
    // Launch browser with remote debugging for Lighthouse
    const browser = await chromium.launch({
      args: [`--remote-debugging-port=${LIGHTHOUSE_PORT}`],
    });

    const page = await browser.newPage();
    await page.goto('http://localhost:8080/', { waitUntil: 'networkidle' });

    // Run Lighthouse audit with performance thresholds
    const result = await playAudit({
      page,
      port: LIGHTHOUSE_PORT,
      thresholds: PERFORMANCE_THRESHOLDS,
      reports: {
        formats: { json: false, html: false, csv: false },
      },
    });

    // Extract performance score from the audit
    const performanceScore = result.lhr.categories.performance.score * 100;

    console.log(`Lighthouse Performance Score: ${performanceScore}`);

    // Assert performance score meets threshold
    expect(performanceScore).toBeGreaterThanOrEqual(90);

    await browser.close();
  });

  test('TC3: First Contentful Paint (FCP) occurs within 1.5 seconds', async () => {
    // Launch browser with remote debugging for Lighthouse
    const browser = await chromium.launch({
      args: [`--remote-debugging-port=${LIGHTHOUSE_PORT}`],
    });

    const page = await browser.newPage();
    await page.goto('http://localhost:8080/', { waitUntil: 'networkidle' });

    // Run Lighthouse audit
    const result = await playAudit({
      page,
      port: LIGHTHOUSE_PORT,
      thresholds: {
        performance: 50, // Lower threshold since we're checking specific metrics
      },
      reports: {
        formats: { json: false, html: false, csv: false },
      },
    });

    // Extract FCP metric from audit results
    const fcpAudit = result.lhr.audits['first-contentful-paint'];
    const fcpMs = fcpAudit.numericValue;

    console.log(`First Contentful Paint: ${fcpMs}ms`);

    // FCP should be within 1.5 seconds (1500ms)
    expect(fcpMs).toBeLessThan(1500);

    await browser.close();
  });

  test('TC4: Largest Contentful Paint (LCP) occurs within 2.5 seconds', async () => {
    // Launch browser with remote debugging for Lighthouse
    const browser = await chromium.launch({
      args: [`--remote-debugging-port=${LIGHTHOUSE_PORT}`],
    });

    const page = await browser.newPage();
    await page.goto('http://localhost:8080/', { waitUntil: 'networkidle' });

    // Run Lighthouse audit
    const result = await playAudit({
      page,
      port: LIGHTHOUSE_PORT,
      thresholds: {
        performance: 50, // Lower threshold since we're checking specific metrics
      },
      reports: {
        formats: { json: false, html: false, csv: false },
      },
    });

    // Extract LCP metric from audit results
    const lcpAudit = result.lhr.audits['largest-contentful-paint'];
    const lcpMs = lcpAudit.numericValue;

    console.log(`Largest Contentful Paint: ${lcpMs}ms`);

    // LCP should be within 2.5 seconds (2500ms)
    expect(lcpMs).toBeLessThan(2500);

    await browser.close();
  });

  test('TC5: Cumulative Layout Shift (CLS) score is below 0.1', async () => {
    // Launch browser with remote debugging for Lighthouse
    const browser = await chromium.launch({
      args: [`--remote-debugging-port=${LIGHTHOUSE_PORT}`],
    });

    const page = await browser.newPage();
    await page.goto('http://localhost:8080/', { waitUntil: 'networkidle' });

    // Run Lighthouse audit
    const result = await playAudit({
      page,
      port: LIGHTHOUSE_PORT,
      thresholds: {
        performance: 50, // Lower threshold since we're checking specific metrics
      },
      reports: {
        formats: { json: false, html: false, csv: false },
      },
    });

    // Extract CLS metric from audit results
    const clsAudit = result.lhr.audits['cumulative-layout-shift'];
    const clsScore = clsAudit.numericValue;

    console.log(`Cumulative Layout Shift: ${clsScore}`);

    // CLS should be below 0.1 for good user experience
    expect(clsScore).toBeLessThan(0.1);

    await browser.close();
  });
});
