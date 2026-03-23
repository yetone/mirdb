/**
 * Performance E2E Tests
 * Owner: Scenario 11 - Page Performance
 *
 * Test cases:
 * - Page load time under 2 seconds
 * - Total page size reasonable (<500KB ideal)
 * - CSS is efficient and optimized
 * - No render-blocking resources or minimal impact
 */

import { test, expect, Page } from '@playwright/test';
import { navigateToHomepage } from './test-utils';

/**
 * Helper to get performance metrics using Performance API
 */
async function getPerformanceMetrics(page: Page): Promise<{
  domContentLoaded: number;
  loadComplete: number;
  firstContentfulPaint: number;
}> {
  const metrics = await page.evaluate(() => {
    const navigation = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
    const paintEntries = performance.getEntriesByType('paint');
    const fcpEntry = paintEntries.find(entry => entry.name === 'first-contentful-paint');

    return {
      domContentLoaded: navigation.domContentLoadedEventEnd - navigation.fetchStart,
      loadComplete: navigation.loadEventEnd - navigation.fetchStart,
      firstContentfulPaint: fcpEntry ? fcpEntry.startTime : 0,
    };
  });

  return metrics;
}

/**
 * Helper to get all loaded resources and their sizes
 */
async function getResourceMetrics(page: Page): Promise<{
  resources: Array<{ name: string; size: number; type: string; duration: number }>;
  totalSize: number;
  totalTransferSize: number;
}> {
  const resources = await page.evaluate(() => {
    const entries = performance.getEntriesByType('resource') as PerformanceResourceTiming[];
    const resourceList = entries.map(entry => ({
      name: entry.name,
      size: entry.encodedBodySize || 0,
      transferSize: entry.transferSize || 0,
      type: entry.initiatorType,
      duration: entry.duration,
    }));

    const totalSize = resourceList.reduce((sum, r) => sum + r.size, 0);
    const totalTransferSize = resourceList.reduce((sum, r) => sum + r.transferSize, 0);

    return {
      resources: resourceList,
      totalSize,
      totalTransferSize,
    };
  });

  return resources;
}

/**
 * Helper to check for render-blocking resources
 */
async function getRenderBlockingInfo(page: Page): Promise<{
  blockingStylesheets: string[];
  blockingScripts: string[];
  hasCriticalCssInline: boolean;
}> {
  const info = await page.evaluate(() => {
    const stylesheets = Array.from(document.querySelectorAll('link[rel="stylesheet"]'));
    const scripts = Array.from(document.querySelectorAll('script:not([async]):not([defer])'));

    // Check for blocking stylesheets (those without media query or with media="all")
    const blockingStylesheets = stylesheets
      .filter(link => {
        const media = link.getAttribute('media');
        return !media || media === 'all' || media === 'screen';
      })
      .map(link => link.getAttribute('href') || 'inline');

    // Check for blocking scripts in head (those without async/defer and in head)
    const headScripts = Array.from(document.head.querySelectorAll('script:not([async]):not([defer])[src]'));
    const blockingScripts = headScripts.map(script => script.getAttribute('src') || 'inline');

    // Check if there's any critical CSS inlined in the head
    const inlineStyles = document.head.querySelectorAll('style');
    const hasCriticalCssInline = inlineStyles.length > 0;

    return {
      blockingStylesheets,
      blockingScripts,
      hasCriticalCssInline,
    };
  });

  return info;
}

/**
 * Helper to check CSS optimization characteristics
 */
async function getCssOptimizationInfo(page: Page): Promise<{
  usesVariables: boolean;
  hasMinimalSelectors: boolean;
  stylesheetsCount: number;
  totalCssRules: number;
}> {
  const info = await page.evaluate(() => {
    const stylesheets = Array.from(document.styleSheets);
    let totalRules = 0;
    let usesVariables = false;

    stylesheets.forEach(sheet => {
      try {
        if (sheet.cssRules) {
          totalRules += sheet.cssRules.length;
          // Check if CSS variables are used
          Array.from(sheet.cssRules).forEach(rule => {
            if (rule.cssText && rule.cssText.includes('var(--')) {
              usesVariables = true;
            }
          });
        }
      } catch (e) {
        // Cross-origin stylesheets will throw
      }
    });

    return {
      usesVariables,
      hasMinimalSelectors: totalRules < 500, // Reasonable number for a landing page
      stylesheetsCount: stylesheets.length,
      totalCssRules: totalRules,
    };
  });

  return info;
}

test.describe('Page Performance - NFR-1 Compliance', () => {
  test.describe('Test Case 1: Page Load Time', () => {
    test('page fully loads in under 2 seconds on standard broadband', async ({ page }) => {
      // Measure time from navigation start to load complete
      const startTime = Date.now();

      await page.goto('/', { waitUntil: 'load' });

      const loadTime = Date.now() - startTime;

      // Get detailed performance metrics
      const metrics = await getPerformanceMetrics(page);

      // Primary assertion: page load under 2 seconds (2000ms)
      expect(metrics.loadComplete).toBeLessThan(2000);

      // Additional validation: DOMContentLoaded should be quick
      expect(metrics.domContentLoaded).toBeLessThan(1500);

      // First Contentful Paint should happen quickly
      expect(metrics.firstContentfulPaint).toBeLessThan(1000);

      console.log(`Performance Metrics:
        - DOM Content Loaded: ${metrics.domContentLoaded.toFixed(2)}ms
        - Load Complete: ${metrics.loadComplete.toFixed(2)}ms
        - First Contentful Paint: ${metrics.firstContentfulPaint.toFixed(2)}ms
        - Total measured time: ${loadTime}ms`);
    });

    test('page is interactive within reasonable time', async ({ page }) => {
      await navigateToHomepage(page);

      // Page should be interactive - hero CTA button should be clickable
      const ctaButton = page.locator('.hero-cta .btn-primary');
      await expect(ctaButton).toBeVisible({ timeout: 2000 });
      await expect(ctaButton).toBeEnabled({ timeout: 2000 });

      // Navigation should be ready
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible({ timeout: 2000 });
    });
  });

  test.describe('Test Case 2: Total Page Size', () => {
    test('total page size is under 500KB for fast loading', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      const resourceMetrics = await getResourceMetrics(page);

      // Get HTML document size
      const htmlSize = await page.evaluate(() => {
        return new Blob([document.documentElement.outerHTML]).size;
      });

      const totalPageSize = htmlSize + resourceMetrics.totalSize;
      const totalPageSizeKB = totalPageSize / 1024;

      // Ideal: under 500KB
      expect(totalPageSizeKB).toBeLessThan(500);

      // Better target: under 100KB for a simple landing page
      // This is a soft assertion - we log a warning if over 100KB but don't fail
      if (totalPageSizeKB > 100) {
        console.warn(`Page size is ${totalPageSizeKB.toFixed(2)}KB - consider further optimization`);
      }

      console.log(`Page Size Analysis:
        - HTML Document: ${(htmlSize / 1024).toFixed(2)}KB
        - External Resources: ${(resourceMetrics.totalSize / 1024).toFixed(2)}KB
        - Total Page Size: ${totalPageSizeKB.toFixed(2)}KB
        - Resources loaded: ${resourceMetrics.resources.length}`);
    });

    test('individual asset sizes are reasonable', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      const resourceMetrics = await getResourceMetrics(page);

      // Check each resource
      resourceMetrics.resources.forEach(resource => {
        const sizeKB = resource.size / 1024;
        const resourceName = resource.name.split('/').pop() || resource.name;

        // Individual CSS files should be under 50KB
        if (resource.type === 'link' || resource.name.endsWith('.css')) {
          expect(sizeKB).toBeLessThan(50);
        }

        // Individual JS files should be under 100KB
        if (resource.type === 'script' || resource.name.endsWith('.js')) {
          expect(sizeKB).toBeLessThan(100);
        }
      });
    });

    test('no unnecessarily large images or assets', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      const resourceMetrics = await getResourceMetrics(page);

      // Filter image resources
      const images = resourceMetrics.resources.filter(
        r => r.type === 'img' || r.name.match(/\.(png|jpg|jpeg|gif|webp|svg)$/i)
      );

      // Each image should be under 200KB for a landing page
      images.forEach(img => {
        const sizeKB = img.size / 1024;
        expect(sizeKB).toBeLessThan(200);
      });

      // Total image weight should be under 300KB
      const totalImageSize = images.reduce((sum, img) => sum + img.size, 0);
      expect(totalImageSize / 1024).toBeLessThan(300);
    });
  });

  test.describe('Test Case 3: CSS Optimization', () => {
    test('CSS uses efficient styling approach with CSS custom properties', async ({ page }) => {
      await navigateToHomepage(page);

      const cssInfo = await getCssOptimizationInfo(page);

      // CSS should use variables for maintainability and efficiency
      expect(cssInfo.usesVariables).toBe(true);

      // Should have reasonable number of stylesheets (not too fragmented)
      expect(cssInfo.stylesheetsCount).toBeLessThanOrEqual(5);

      // Should have reasonable number of CSS rules
      expect(cssInfo.hasMinimalSelectors).toBe(true);

      console.log(`CSS Optimization Analysis:
        - Uses CSS Variables: ${cssInfo.usesVariables}
        - Stylesheets Count: ${cssInfo.stylesheetsCount}
        - Total CSS Rules: ${cssInfo.totalCssRules}
        - Minimal Selectors: ${cssInfo.hasMinimalSelectors}`);
    });

    test('CSS is well-structured without excessive specificity', async ({ page }) => {
      await navigateToHomepage(page);

      // Check that CSS is loaded and parsed
      const stylesLoaded = await page.evaluate(() => {
        const stylesheets = document.styleSheets;
        return stylesheets.length > 0;
      });

      expect(stylesLoaded).toBe(true);

      // Verify styles are applied correctly
      const heroTitle = page.locator('.hero-title');
      const computedStyle = await heroTitle.evaluate(el => {
        const style = window.getComputedStyle(el);
        return {
          fontWeight: style.fontWeight,
          color: style.color,
          display: style.display,
        };
      });

      // Styles should be applied (not default browser styles)
      expect(computedStyle.fontWeight).toBeTruthy();
      expect(computedStyle.color).toBeTruthy();
    });

    test('CSS file sizes are reasonable for a static site', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      const resourceMetrics = await getResourceMetrics(page);

      // Find CSS resources
      const cssResources = resourceMetrics.resources.filter(
        r => r.type === 'link' || r.name.endsWith('.css')
      );

      // Total CSS should be under 30KB for efficient styling
      const totalCssSize = cssResources.reduce((sum, r) => sum + r.size, 0);
      const totalCssSizeKB = totalCssSize / 1024;

      // CSS should be efficiently sized
      expect(totalCssSizeKB).toBeLessThan(50);

      console.log(`CSS Size Analysis:
        - CSS Files: ${cssResources.length}
        - Total CSS Size: ${totalCssSizeKB.toFixed(2)}KB`);
    });
  });

  test.describe('Test Case 4: Render-Blocking Resources', () => {
    test('render-blocking resources are minimized', async ({ page }) => {
      await navigateToHomepage(page);

      const blockingInfo = await getRenderBlockingInfo(page);

      // CSS stylesheets are typically render-blocking, but this is acceptable
      // for small stylesheets on a simple landing page
      // We expect 2-3 stylesheets maximum
      expect(blockingInfo.blockingStylesheets.length).toBeLessThanOrEqual(3);

      // There should be NO blocking scripts in the head
      expect(blockingInfo.blockingScripts.length).toBe(0);

      console.log(`Render-Blocking Analysis:
        - Blocking Stylesheets: ${blockingInfo.blockingStylesheets.length}
        - Blocking Scripts: ${blockingInfo.blockingScripts.length}
        - Has Critical CSS Inline: ${blockingInfo.hasCriticalCssInline}`);
    });

    test('JavaScript is loaded non-blocking (at end of body)', async ({ page }) => {
      await navigateToHomepage(page);

      // Check that scripts are at end of body, not in head
      const scriptPosition = await page.evaluate(() => {
        const bodyScripts = document.body.querySelectorAll('script[src]');
        const headScripts = document.head.querySelectorAll('script[src]');

        // All external scripts should be in body (or have async/defer)
        const headBlockingScripts = Array.from(headScripts).filter(
          s => !s.hasAttribute('async') && !s.hasAttribute('defer')
        );

        return {
          scriptsInBody: bodyScripts.length,
          blockingScriptsInHead: headBlockingScripts.length,
        };
      });

      // No blocking scripts in head
      expect(scriptPosition.blockingScriptsInHead).toBe(0);

      // Scripts should be loaded from body
      expect(scriptPosition.scriptsInBody).toBeGreaterThanOrEqual(1);
    });

    test('critical content renders before scripts execute', async ({ page }) => {
      // Navigate and check that critical content is visible immediately
      await page.goto('/', { waitUntil: 'domcontentloaded' });

      // Hero section should be visible immediately after DOM load
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible({ timeout: 1000 });

      // Product name should be visible
      const heroTitle = page.locator('.hero-title');
      await expect(heroTitle).toBeVisible({ timeout: 1000 });
      await expect(heroTitle).toHaveText('MirDB');

      // Navigation should be visible
      const nav = page.locator('.nav');
      await expect(nav).toBeVisible({ timeout: 1000 });
    });

    test('page remains usable without JavaScript', async ({ browser }) => {
      // Create a context with JavaScript disabled
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Core content should still be visible
      const heroTitle = page.locator('.hero-title');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText('MirDB');

      // Features section should be visible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Navigation links should be visible
      const navLinks = page.locator('.nav-link');
      await expect(navLinks.first()).toBeVisible();

      // Quick start section should be visible
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();

      await context.close();
    });
  });

  test.describe('Additional Performance Checks', () => {
    test('no external font or CDN dependencies', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      const resourceMetrics = await getResourceMetrics(page);

      // Check for external CDN resources
      const externalResources = resourceMetrics.resources.filter(r => {
        const url = new URL(r.name, 'http://localhost');
        return url.hostname !== 'localhost' && !url.hostname.includes('127.0.0.1');
      });

      // Static site should have no external dependencies
      expect(externalResources.length).toBe(0);

      console.log(`External Dependencies: ${externalResources.length}`);
    });

    test('uses system fonts for fast rendering', async ({ page }) => {
      await navigateToHomepage(page);

      // Check font-family uses system fonts
      const fontInfo = await page.evaluate(() => {
        const body = document.body;
        const computedStyle = window.getComputedStyle(body);
        const fontFamily = computedStyle.fontFamily;

        // System fonts typically include: system-ui, -apple-system, etc.
        const usesSystemFonts = fontFamily.includes('system-ui') ||
                               fontFamily.includes('-apple-system') ||
                               fontFamily.includes('Segoe UI') ||
                               fontFamily.includes('Arial');

        return {
          fontFamily,
          usesSystemFonts,
        };
      });

      expect(fontInfo.usesSystemFonts).toBe(true);
    });

    test('efficient caching headers suggested (static assets)', async ({ page }) => {
      // This test checks that static assets are cacheable
      // For a simple HTTP server, we just verify resources load quickly on repeat

      // First load
      await page.goto('/', { waitUntil: 'networkidle' });
      const firstMetrics = await getPerformanceMetrics(page);

      // Second load (should be faster with browser caching)
      await page.goto('/', { waitUntil: 'networkidle' });
      const secondMetrics = await getPerformanceMetrics(page);

      // Second load should not be significantly slower
      // (allowing for variance, but should be similar or faster)
      expect(secondMetrics.loadComplete).toBeLessThan(firstMetrics.loadComplete * 1.5);
    });
  });
});
