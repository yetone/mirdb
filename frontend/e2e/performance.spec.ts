import { test, expect } from '@playwright/test';

/**
 * Performance tests for the homepage based on NFR-1 requirements:
 * - Homepage shall load initial content within 2 seconds
 * - Tests Core Web Vitals metrics: FCP, LCP, CLS
 */
test.describe('Homepage Performance - Core Web Vitals', () => {
  test.beforeEach(async ({ context }) => {
    // Clear browser cache to ensure cold start measurement
    await context.clearCookies();
  });

  test('First Contentful Paint (FCP) should be under 1.8 seconds', async ({ page }) => {
    // Navigate to homepage and collect performance metrics
    await page.goto('/', { waitUntil: 'networkidle' });

    // Get FCP from Performance API
    const fcp = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntries();
          for (const entry of entries) {
            if (entry.name === 'first-contentful-paint') {
              resolve(entry.startTime);
            }
          }
        });

        observer.observe({ type: 'paint', buffered: true });

        // Check if FCP is already available in buffered entries
        const paintEntries = performance.getEntriesByType('paint');
        const fcpEntry = paintEntries.find((e) => e.name === 'first-contentful-paint');
        if (fcpEntry) {
          resolve(fcpEntry.startTime);
        }

        // Timeout fallback - if no FCP entry found within 5 seconds
        setTimeout(() => {
          // Still resolve with a value that will fail the test
          resolve(5000);
        }, 5000);
      });
    });

    // FCP should be under 1.8 seconds (1800ms)
    expect(fcp).toBeLessThan(1800);
  });

  test('Largest Contentful Paint (LCP) should be under 2.5 seconds', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Wait for hero section to be visible (main LCP element)
    await expect(page.getByTestId('hero-section')).toBeVisible();

    // Get LCP from Performance API
    const lcp = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        let lcpValue = 0;

        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntries();
          // LCP reports multiple times, the last one is the final LCP
          for (const entry of entries) {
            lcpValue = entry.startTime;
          }
        });

        observer.observe({ type: 'largest-contentful-paint', buffered: true });

        // Wait a bit to capture the final LCP value
        setTimeout(() => {
          observer.disconnect();
          resolve(lcpValue);
        }, 2000);
      });
    });

    // LCP should be under 2.5 seconds (2500ms)
    expect(lcp).toBeLessThan(2500);
  });

  test('Cumulative Layout Shift (CLS) should be under 0.1', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Wait for page to stabilize
    await page.waitForLoadState('networkidle');

    // Scroll through the page to trigger any layout shifts
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight / 2));
    await page.waitForTimeout(500);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);

    // Get CLS from Performance API
    const cls = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        let clsValue = 0;

        const observer = new PerformanceObserver((list) => {
          for (const entry of list.getEntries()) {
            // CLS entries have hadRecentInput property
            const layoutShift = entry as PerformanceEntry & {
              hadRecentInput: boolean;
              value: number;
            };
            if (!layoutShift.hadRecentInput) {
              clsValue += layoutShift.value;
            }
          }
        });

        observer.observe({ type: 'layout-shift', buffered: true });

        // Wait a bit to capture all layout shifts
        setTimeout(() => {
          observer.disconnect();
          resolve(clsValue);
        }, 1500);
      });
    });

    // CLS should be under 0.1
    expect(cls).toBeLessThan(0.1);
  });

  test('Hero section content is visible within 2 seconds', async ({ page }) => {
    const startTime = Date.now();

    // Navigate to homepage
    await page.goto('/');

    // Wait for hero section content to be visible
    const heroSection = page.getByTestId('hero-section');
    const headline = page.getByRole('heading', { level: 1 });
    const ctaButtons = page.getByTestId('cta-get-started');

    await expect(heroSection).toBeVisible();
    await expect(headline).toBeVisible();
    await expect(ctaButtons).toBeVisible();

    const loadTime = Date.now() - startTime;

    // Content should be visible within 2 seconds (NFR-1 requirement)
    expect(loadTime).toBeLessThan(2000);
  });

  test('Page navigation metrics are acceptable', async ({ page }) => {
    // Navigate to homepage and collect navigation timing
    await page.goto('/', { waitUntil: 'load' });

    const navigationTiming = await page.evaluate(() => {
      const timing = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      return {
        // Time to first byte
        ttfb: timing.responseStart - timing.requestStart,
        // DOM Interactive
        domInteractive: timing.domInteractive,
        // DOM Content Loaded
        domContentLoaded: timing.domContentLoadedEventEnd,
        // Total load time
        loadComplete: timing.loadEventEnd,
      };
    });

    // TTFB should be reasonable (under 800ms for development server)
    expect(navigationTiming.ttfb).toBeLessThan(800);

    // DOM should be interactive quickly
    expect(navigationTiming.domInteractive).toBeLessThan(2000);

    // DOM Content Loaded event should fire within 2 seconds
    expect(navigationTiming.domContentLoaded).toBeLessThan(2000);
  });
});
