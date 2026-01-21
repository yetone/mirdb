/**
 * E2E Performance Tests for Page Load
 * Tests NFR-2: Page load time under 3 seconds
 *
 * Test cases:
 * - TC1: Page fully loads within 3 seconds
 * - TC2: First Contentful Paint (FCP) occurs within 1.8 seconds
 * - TC4: Cumulative Layout Shift (CLS) score is below 0.1
 */

import { test, expect } from '@playwright/test';

test.describe('Page Load Performance - E2E Tests', () => {
  test.describe('Test Case 1: Page Load Time', () => {
    test('Page should fully load within 3 seconds on standard connection', async ({ page }) => {
      // Start timing
      const startTime = Date.now();

      // Navigate to the page and wait for load event
      await page.goto('/', { waitUntil: 'load' });

      const loadTime = Date.now() - startTime;

      // Page should load within 3000ms (3 seconds)
      expect(loadTime).toBeLessThan(3000);

      // Verify core content is visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();
    });

    test('Page should reach network idle within 3 seconds', async ({ page }) => {
      const startTime = Date.now();

      // Wait for network to be idle (no requests for 500ms)
      await page.goto('/', { waitUntil: 'networkidle' });

      const loadTime = Date.now() - startTime;

      // Should complete within 3 seconds
      expect(loadTime).toBeLessThan(3000);
    });

    test('DOM content loaded event fires within 2 seconds', async ({ page }) => {
      const startTime = Date.now();

      await page.goto('/', { waitUntil: 'domcontentloaded' });

      const loadTime = Date.now() - startTime;

      // DOM should be ready within 2 seconds
      expect(loadTime).toBeLessThan(2000);
    });
  });

  test.describe('Test Case 2: First Contentful Paint', () => {
    test('First Contentful Paint (FCP) should occur within 1.8 seconds', async ({ page }) => {
      // Navigate to the page
      await page.goto('/');

      // Get performance metrics using Performance API
      const metrics = await page.evaluate(() => {
        return new Promise((resolve) => {
          // Wait a bit for paint entries to be recorded
          setTimeout(() => {
            const paintEntries = performance.getEntriesByType('paint');
            const fcpEntry = paintEntries.find(entry => entry.name === 'first-contentful-paint');
            resolve({
              fcp: fcpEntry ? fcpEntry.startTime : null,
              entries: paintEntries.map(e => ({ name: e.name, startTime: e.startTime }))
            });
          }, 1000);
        });
      });

      // FCP should occur within 1800ms (1.8 seconds)
      if (metrics.fcp !== null) {
        expect(metrics.fcp).toBeLessThan(1800);
      } else {
        // If FCP is not available, verify content is visible quickly
        const heroHeadline = page.locator('[data-testid="hero-headline"]');
        await expect(heroHeadline).toBeVisible({ timeout: 1800 });
      }
    });

    test('Hero section content should be visible within 1.8 seconds', async ({ page }) => {
      const startTime = Date.now();

      await page.goto('/');

      // Check hero headline is visible
      const heroHeadline = page.locator('[data-testid="hero-headline"]');
      await expect(heroHeadline).toBeVisible({ timeout: 1800 });

      const timeToVisible = Date.now() - startTime;
      expect(timeToVisible).toBeLessThan(1800);
    });

    test('Primary CTA button should be visible within 1.8 seconds', async ({ page }) => {
      const startTime = Date.now();

      await page.goto('/');

      // Check primary CTA is visible
      const primaryCta = page.locator('[data-testid="hero-cta-primary"]');
      await expect(primaryCta).toBeVisible({ timeout: 1800 });

      const timeToVisible = Date.now() - startTime;
      expect(timeToVisible).toBeLessThan(1800);
    });
  });

  test.describe('Test Case 4: Cumulative Layout Shift', () => {
    test('CLS score should be below 0.1', async ({ page }) => {
      // Navigate to the page
      await page.goto('/');

      // Measure CLS using PerformanceObserver
      const clsScore = await page.evaluate(() => {
        return new Promise((resolve) => {
          let cls = 0;

          // Create a performance observer for layout-shift entries
          const observer = new PerformanceObserver((entryList) => {
            for (const entry of entryList.getEntries()) {
              // Only count entries without user input
              if (!entry.hadRecentInput) {
                cls += entry.value;
              }
            }
          });

          observer.observe({ type: 'layout-shift', buffered: true });

          // Wait for page to stabilize and then return CLS
          setTimeout(() => {
            observer.disconnect();
            resolve(cls);
          }, 3000);
        });
      });

      // CLS should be below 0.1 (good score per Web Vitals)
      expect(clsScore).toBeLessThan(0.1);
    });

    test('No noticeable layout shifts during initial load', async ({ page }) => {
      await page.goto('/');

      // Check that hero section maintains its position
      const heroSection = page.locator('[data-testid="hero-section"]');
      const initialBox = await heroSection.boundingBox();

      // Wait for potential layout shifts
      await page.waitForTimeout(1000);

      const finalBox = await heroSection.boundingBox();

      // Position should not have shifted significantly
      if (initialBox && finalBox) {
        expect(Math.abs(finalBox.y - initialBox.y)).toBeLessThan(5);
        expect(Math.abs(finalBox.x - initialBox.x)).toBeLessThan(5);
      }
    });

    test('Header remains stable after page load', async ({ page }) => {
      await page.goto('/');

      const header = page.locator('[data-testid="header"]');
      const initialBox = await header.boundingBox();

      // Scroll down and back up
      await page.evaluate(() => window.scrollTo(0, 500));
      await page.waitForTimeout(500);
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(500);

      const finalBox = await header.boundingBox();

      // Header dimensions should remain stable
      if (initialBox && finalBox) {
        expect(finalBox.height).toBe(initialBox.height);
      }
    });
  });

  test.describe('Image Lazy Loading E2E Verification', () => {
    test('Below-fold images should not load until scrolled into view', async ({ page }) => {
      // Navigate without scrolling
      await page.goto('/');

      // Get the showcase image loading status before scroll
      const initialLoadState = await page.evaluate(() => {
        const img = document.querySelector('[data-testid="showcase-image"]');
        return {
          hasLazyLoading: img?.getAttribute('loading') === 'lazy',
          complete: img?.complete,
          naturalHeight: img?.naturalHeight
        };
      });

      expect(initialLoadState.hasLazyLoading).toBe(true);

      // Scroll to the showcase section
      await page.locator('#showcase').scrollIntoViewIfNeeded();
      await page.waitForTimeout(1000);

      // Now the image should be loaded or loading
      const afterScrollState = await page.evaluate(() => {
        const img = document.querySelector('[data-testid="showcase-image"]');
        return {
          complete: img?.complete,
          naturalHeight: img?.naturalHeight
        };
      });

      // Image should be complete or have natural height after scroll
      expect(afterScrollState.complete || afterScrollState.naturalHeight > 0).toBe(true);
    });

    test('Hero image loads immediately (above the fold)', async ({ page }) => {
      await page.goto('/');

      // Wait a short time for initial render
      await page.waitForTimeout(500);

      const heroImageState = await page.evaluate(() => {
        const img = document.querySelector('[data-testid="hero-image"]');
        return {
          loadingAttr: img?.getAttribute('loading'),
          complete: img?.complete,
          naturalHeight: img?.naturalHeight
        };
      });

      // Hero image should either not have lazy loading or be already loaded
      expect(heroImageState.loadingAttr !== 'lazy' || heroImageState.complete).toBe(true);
    });
  });

  test.describe('Resource Loading Optimization', () => {
    test('Critical CSS should not block rendering', async ({ page }) => {
      const startTime = Date.now();

      await page.goto('/');

      // First paint should happen quickly even with CSS
      const firstPaint = await page.evaluate(() => {
        const paintEntries = performance.getEntriesByType('paint');
        const fpEntry = paintEntries.find(entry => entry.name === 'first-paint');
        return fpEntry ? fpEntry.startTime : null;
      });

      // First paint within 1.5 seconds indicates CSS is not blocking
      if (firstPaint !== null) {
        expect(firstPaint).toBeLessThan(1500);
      }
    });

    test('JavaScript does not block critical content', async ({ page }) => {
      await page.goto('/');

      // Core content should be visible even before JS fully loads
      const heroHeadline = page.locator('[data-testid="hero-headline"]');
      await expect(heroHeadline).toBeVisible({ timeout: 2000 });

      // Navigation should be visible
      const header = page.locator('[data-testid="header"]');
      await expect(header).toBeVisible({ timeout: 2000 });
    });
  });
});
