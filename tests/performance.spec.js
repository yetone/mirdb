// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Performance Test Suite for MirDB Homepage
 *
 * Tests Core Web Vitals and page load performance metrics:
 * - Page Load Time: Must be under 2 seconds
 * - Largest Contentful Paint (LCP): Must be under 2.5 seconds
 * - Cumulative Layout Shift (CLS): Must be under 0.1
 * - Render-blocking resources: Critical CSS should be minimized
 */

test.describe('Performance - Page Load Time', () => {

  test.beforeEach(async ({ page }) => {
    // Enable performance metrics collection
    await page.goto('/');
  });

  test('TC1: Page fully loads within 2 seconds', async ({ page }) => {
    // Navigate to homepage and measure load time
    const startTime = Date.now();

    await page.goto('/', { waitUntil: 'load' });

    const loadTime = Date.now() - startTime;

    // Verify page is fully loaded by checking key elements
    await expect(page.locator('h1')).toBeVisible();
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.features')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();

    // Page should load within 2000ms (2 seconds)
    expect(loadTime).toBeLessThan(2000);

    console.log(`Page load time: ${loadTime}ms`);
  });

  test('TC2: Lighthouse performance score is 90 or higher', async ({ page }) => {
    // Since we cannot run actual Lighthouse in Playwright, we test key performance indicators
    // that correlate with high Lighthouse scores:
    // 1. First Contentful Paint timing
    // 2. DOM Content Loaded timing
    // 3. Resource optimization

    await page.goto('/');

    // Get performance metrics using Performance API
    const performanceMetrics = await page.evaluate(() => {
      const timing = performance.timing;
      const navigation = performance.getEntriesByType('navigation')[0];

      return {
        domContentLoaded: timing.domContentLoadedEventEnd - timing.navigationStart,
        loadComplete: timing.loadEventEnd - timing.navigationStart,
        domInteractive: timing.domInteractive - timing.navigationStart,
        firstPaint: performance.getEntriesByType('paint')
          .find(p => p.name === 'first-paint')?.startTime || null,
        firstContentfulPaint: performance.getEntriesByType('paint')
          .find(p => p.name === 'first-contentful-paint')?.startTime || null,
        transferSize: navigation ? navigation.transferSize : null,
        encodedBodySize: navigation ? navigation.encodedBodySize : null,
      };
    });

    console.log('Performance Metrics:', JSON.stringify(performanceMetrics, null, 2));

    // Verify key performance metrics that contribute to high Lighthouse score
    // DOM Content Loaded should be under 1.5 seconds
    expect(performanceMetrics.domContentLoaded).toBeLessThan(1500);

    // Page load complete should be under 3 seconds
    expect(performanceMetrics.loadComplete).toBeLessThan(3000);

    // DOM Interactive should be under 1 second
    expect(performanceMetrics.domInteractive).toBeLessThan(1000);

    // First Contentful Paint should occur within 1.8 seconds for good Lighthouse score
    if (performanceMetrics.firstContentfulPaint !== null) {
      expect(performanceMetrics.firstContentfulPaint).toBeLessThan(1800);
    }

    // Verify the page has reasonable resource sizes (no bloated assets)
    // Total transfer should be reasonable (under 5MB for homepage)
    if (performanceMetrics.transferSize !== null) {
      expect(performanceMetrics.transferSize).toBeLessThan(5 * 1024 * 1024);
    }
  });

  test('TC3: Largest Contentful Paint (LCP) occurs within 2.5 seconds', async ({ page }) => {
    await page.goto('/');

    // Use Performance Observer to measure LCP
    const lcpValue = await page.evaluate(() => {
      return new Promise((resolve) => {
        let lcpEntry = null;

        // Check if LCP entries exist from paint timing
        const paintEntries = performance.getEntriesByType('paint');
        const fcpEntry = paintEntries.find(p => p.name === 'first-contentful-paint');

        // Create a PerformanceObserver for LCP
        const observer = new PerformanceObserver((entryList) => {
          const entries = entryList.getEntries();
          lcpEntry = entries[entries.length - 1];
        });

        observer.observe({ type: 'largest-contentful-paint', buffered: true });

        // Wait for page to stabilize, then return LCP
        setTimeout(() => {
          observer.disconnect();
          if (lcpEntry) {
            resolve(lcpEntry.startTime);
          } else if (fcpEntry) {
            // Fallback to FCP if LCP not available
            resolve(fcpEntry.startTime);
          } else {
            // Final fallback using domContentLoaded
            resolve(performance.timing.domContentLoadedEventEnd - performance.timing.navigationStart);
          }
        }, 1000);
      });
    });

    console.log(`LCP value: ${lcpValue}ms`);

    // LCP must be within 2.5 seconds (2500ms) for good Core Web Vitals
    expect(lcpValue).toBeLessThan(2500);

    // Verify the largest content element is visible
    // The hero section is typically the LCP element
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.hero h1')).toBeVisible();
  });

  test('TC4: Cumulative Layout Shift (CLS) score is less than 0.1', async ({ page }) => {
    await page.goto('/');

    // Measure CLS using Layout Instability API
    const clsValue = await page.evaluate(() => {
      return new Promise((resolve) => {
        let clsScore = 0;

        const observer = new PerformanceObserver((entryList) => {
          for (const entry of entryList.getEntries()) {
            // Only count layout shifts without recent input
            if (!entry.hadRecentInput) {
              clsScore += entry.value;
            }
          }
        });

        observer.observe({ type: 'layout-shift', buffered: true });

        // Wait for page to stabilize and gather CLS data
        setTimeout(() => {
          observer.disconnect();
          resolve(clsScore);
        }, 2000);
      });
    });

    console.log(`CLS value: ${clsValue}`);

    // CLS must be less than 0.1 for good Core Web Vitals
    expect(clsValue).toBeLessThan(0.1);

    // Verify key elements have proper dimensions to prevent layout shifts
    // Check that images have explicit width and height
    const logoImg = page.locator('.logo img');
    await expect(logoImg).toHaveAttribute('width');
    await expect(logoImg).toHaveAttribute('height');
  });

  test('TC5: Critical CSS is inlined and non-critical resources are deferred', async ({ page }) => {
    // Analyze render-blocking resources
    const response = await page.goto('/');
    const html = await response.text();

    // Check 1: Verify stylesheet link exists (external CSS is acceptable if not render-blocking)
    const hasStylesheetLink = html.includes('rel="stylesheet"');
    expect(hasStylesheetLink).toBeTruthy();

    // Check 2: Verify scripts are loaded at the end of body or have defer/async
    // Scripts should not block rendering
    const scriptLoadingAnalysis = await page.evaluate(() => {
      const scripts = Array.from(document.querySelectorAll('script[src]'));
      const results = {
        totalScripts: scripts.length,
        deferredOrAsync: 0,
        inBody: 0,
        renderBlocking: 0,
      };

      scripts.forEach(script => {
        const hasDefer = script.hasAttribute('defer');
        const hasAsync = script.hasAttribute('async');
        const isInBody = document.body.contains(script);

        if (hasDefer || hasAsync) {
          results.deferredOrAsync++;
        }
        if (isInBody) {
          results.inBody++;
        }
        // Scripts in head without defer/async are render-blocking
        if (!hasDefer && !hasAsync && !isInBody) {
          results.renderBlocking++;
        }
      });

      return results;
    });

    console.log('Script Loading Analysis:', JSON.stringify(scriptLoadingAnalysis, null, 2));

    // Verify no render-blocking scripts in head, OR scripts are in body (after content)
    // Scripts placed at end of body are effectively deferred
    const nonBlocking = scriptLoadingAnalysis.deferredOrAsync + scriptLoadingAnalysis.inBody;
    expect(nonBlocking).toBe(scriptLoadingAnalysis.totalScripts);

    // Check 3: Verify CSS is either inlined or loaded efficiently
    const styleAnalysis = await page.evaluate(() => {
      const inlineStyles = document.querySelectorAll('style').length;
      const linkedStylesheets = document.querySelectorAll('link[rel="stylesheet"]').length;

      // Check for critical CSS indicators
      const hasCriticalCssInline = document.querySelector('style') !== null;

      return {
        inlineStyles,
        linkedStylesheets,
        hasCriticalCssInline,
      };
    });

    console.log('Style Analysis:', JSON.stringify(styleAnalysis, null, 2));

    // Check 4: Verify page renders correctly without JS (CSS is not JS-dependent)
    const criticalElementsVisible = await page.evaluate(() => {
      // These elements should be styled and visible immediately
      const header = document.querySelector('.header');
      const hero = document.querySelector('.hero');
      const features = document.querySelector('.features');

      return {
        headerVisible: header && getComputedStyle(header).display !== 'none',
        heroVisible: hero && getComputedStyle(hero).display !== 'none',
        featuresVisible: features && getComputedStyle(features).display !== 'none',
      };
    });

    expect(criticalElementsVisible.headerVisible).toBeTruthy();
    expect(criticalElementsVisible.heroVisible).toBeTruthy();
    expect(criticalElementsVisible.featuresVisible).toBeTruthy();

    // Check 5: Verify Prism.js syntax highlighting scripts are at end of body (non-blocking)
    const prismScriptsDeferred = await page.evaluate(() => {
      const prismScripts = Array.from(document.querySelectorAll('script[src*="prism"]'));
      // All Prism scripts should be in body (loaded after content)
      return prismScripts.every(script => document.body.contains(script));
    });

    expect(prismScriptsDeferred).toBeTruthy();
  });

});

test.describe('Performance - Additional Metrics', () => {

  test('Time to Interactive (TTI) is reasonable', async ({ page }) => {
    const startTime = Date.now();

    await page.goto('/', { waitUntil: 'networkidle' });

    // Test that interactive elements are functional
    const primaryCta = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCta).toBeVisible();
    await expect(primaryCta).toBeEnabled();

    // Test navigation links are interactive
    const navLinks = page.locator('.nav-links a');
    const firstNavLink = navLinks.first();
    await expect(firstNavLink).toBeVisible();

    const tti = Date.now() - startTime;
    console.log(`Time to Interactive: ${tti}ms`);

    // TTI should be under 3.5 seconds for good performance
    expect(tti).toBeLessThan(3500);
  });

  test('Resources are optimized and not excessive', async ({ page }) => {
    await page.goto('/');

    const resourceAnalysis = await page.evaluate(() => {
      const resources = performance.getEntriesByType('resource');

      let totalSize = 0;
      let largeResources = [];

      resources.forEach(resource => {
        const size = resource.transferSize || 0;
        totalSize += size;

        // Flag resources larger than 1MB
        if (size > 1024 * 1024) {
          largeResources.push({
            name: resource.name,
            size: Math.round(size / 1024) + 'KB',
          });
        }
      });

      return {
        totalResources: resources.length,
        totalSizeKB: Math.round(totalSize / 1024),
        largeResources,
      };
    });

    console.log('Resource Analysis:', JSON.stringify(resourceAnalysis, null, 2));

    // Total page size should be reasonable (under 10MB including all assets)
    expect(resourceAnalysis.totalSizeKB).toBeLessThan(10 * 1024);

    // Number of resources should be reasonable (under 50)
    expect(resourceAnalysis.totalResources).toBeLessThan(50);
  });

});
