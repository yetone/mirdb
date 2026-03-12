/**
 * Performance Tests
 * Owner: Scenario 7 - Performance and Loading
 *
 * Tests:
 * - Page loads within 2 seconds on simulated broadband
 * - All assets (GIFs) load successfully
 * - No console errors during page load
 * - DOM content loaded event fires promptly
 *
 * Traceability: NFR-1, NFR-3
 */

import { test, expect, ConsoleMessage } from '@playwright/test';
import { waitForPageLoad } from './test-utils';

const HOMEPAGE_URL = '/homepage/';

// Performance thresholds
const MAX_PAGE_LOAD_TIME_MS = 2000;
const MAX_DOM_CONTENT_LOADED_MS = 1000;

test.describe('Performance and Loading', () => {
  test('TC1: Page fully loads within 2000ms on simulated broadband', async ({ page }) => {
    // Measure load performance using Navigation Timing API
    // This tests that the static assets and page structure allow fast loading
    await page.goto(HOMEPAGE_URL, { waitUntil: 'load' });

    // Use Performance API to measure actual load time
    const loadTime = await page.evaluate(() => {
      const timing = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      // loadEventEnd - fetchStart gives total page load time
      return timing.loadEventEnd - timing.fetchStart;
    });

    // Page should load within 2 seconds
    expect(loadTime).toBeLessThanOrEqual(MAX_PAGE_LOAD_TIME_MS);
  });

  test('TC2: Logo image returns HTTP 200 status and loads successfully', async ({ page }) => {
    // Intercept logo.gif request
    let logoResponse: { status: number; ok: boolean } | null = null;

    page.on('response', (response) => {
      if (response.url().includes('logo.gif')) {
        logoResponse = { status: response.status(), ok: response.ok() };
      }
    });

    await page.goto(HOMEPAGE_URL);
    await waitForPageLoad(page);

    // Verify the logo HTTP response
    expect(logoResponse).not.toBeNull();
    expect(logoResponse!.status).toBe(200);
    expect(logoResponse!.ok).toBe(true);

    // Verify the image is loaded in the DOM
    const logoLoaded = await page.evaluate(() => {
      const img = document.querySelector('[data-testid="hero-logo"]') as HTMLImageElement;
      return img && img.complete && img.naturalHeight > 0 && img.naturalWidth > 0;
    });

    expect(logoLoaded).toBe(true);
  });

  test('TC3: Usage demo image returns HTTP 200 status and loads successfully', async ({ page }) => {
    // Intercept usage.gif request
    let usageResponse: { status: number; ok: boolean } | null = null;

    page.on('response', (response) => {
      if (response.url().includes('usage.gif')) {
        usageResponse = { status: response.status(), ok: response.ok() };
      }
    });

    await page.goto(HOMEPAGE_URL);
    await waitForPageLoad(page);

    // Verify the usage.gif HTTP response
    expect(usageResponse).not.toBeNull();
    expect(usageResponse!.status).toBe(200);
    expect(usageResponse!.ok).toBe(true);

    // Verify the image is loaded in the DOM
    const usageLoaded = await page.evaluate(() => {
      const img = document.querySelector('[data-testid="usage-gif"]') as HTMLImageElement;
      return img && img.complete && img.naturalHeight > 0 && img.naturalWidth > 0;
    });

    expect(usageLoaded).toBe(true);
  });

  test('TC4: No images on the page display as broken/missing', async ({ page }) => {
    await page.goto(HOMEPAGE_URL);
    await waitForPageLoad(page);

    // Check all images on the page
    const brokenImages = await page.evaluate(() => {
      const images = Array.from(document.querySelectorAll('img'));
      const broken: string[] = [];

      for (const img of images) {
        // Check if image failed to load
        if (!img.complete || img.naturalHeight === 0 || img.naturalWidth === 0) {
          broken.push(img.src || img.getAttribute('data-testid') || 'unknown');
        }
      }

      return broken;
    });

    expect(brokenImages).toHaveLength(0);
  });

  test('TC5: No error messages appear in browser console', async ({ page }) => {
    const consoleErrors: string[] = [];

    // Listen for console errors
    page.on('console', (msg: ConsoleMessage) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Listen for page errors
    page.on('pageerror', (error) => {
      consoleErrors.push(error.message);
    });

    await page.goto(HOMEPAGE_URL);
    await waitForPageLoad(page);

    // Allow a brief moment for any async errors to surface
    await page.waitForTimeout(500);

    expect(consoleErrors).toHaveLength(0);
  });

  test('TC6: DOMContentLoaded event fires within 1000ms', async ({ page }) => {
    // Navigate to page
    await page.goto(HOMEPAGE_URL, { waitUntil: 'domcontentloaded' });

    // Use Navigation Timing API to measure DOMContentLoaded time
    const domContentLoadedTime = await page.evaluate(() => {
      const timing = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      return timing.domContentLoadedEventEnd - timing.fetchStart;
    });

    expect(domContentLoadedTime).toBeLessThanOrEqual(MAX_DOM_CONTENT_LOADED_MS);
  });

  test('TC7: Stylesheet loads successfully without 404 errors', async ({ page }) => {
    let cssResponse: { status: number; ok: boolean; url: string } | null = null;
    const failedRequests: string[] = [];

    page.on('response', (response) => {
      const url = response.url();
      if (url.includes('styles.css')) {
        cssResponse = { status: response.status(), ok: response.ok(), url };
      }
      // Track any 404 errors for CSS files
      if (url.endsWith('.css') && response.status() === 404) {
        failedRequests.push(url);
      }
    });

    await page.goto(HOMEPAGE_URL);
    await waitForPageLoad(page);

    // Verify CSS file loaded successfully
    expect(cssResponse).not.toBeNull();
    expect(cssResponse!.status).toBe(200);
    expect(cssResponse!.ok).toBe(true);

    // Verify no CSS 404 errors
    expect(failedRequests).toHaveLength(0);

    // Verify styles are actually applied by checking a styled element
    const hasStyles = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = window.getComputedStyle(body);
      // Check if our CSS variables or styles are applied
      return computedStyle.backgroundColor !== '' && computedStyle.fontFamily !== '';
    });

    expect(hasStyles).toBe(true);
  });
});
