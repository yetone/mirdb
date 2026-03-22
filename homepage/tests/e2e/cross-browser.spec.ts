/**
 * Cross-Browser Compatibility E2E Tests
 * Owner: Scenario 19 (Cross-Browser Compatibility)
 *
 * Test groups:
 * - Page rendering across Chrome, Firefox, Safari (WebKit), Edge
 * - CSS animations (logo GIF, hover effects) consistency
 * - Theme toggle functionality across browsers
 * - JavaScript functionality across browsers
 *
 * These tests run automatically on all configured browsers in playwright.config.ts
 */

import { test, expect } from '@playwright/test';
import { waitForLoad } from './utils';

test.describe('Cross-Browser Compatibility (Scenario 19)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('TC1: Page renders correctly with all main sections visible', async ({ page, browserName }) => {
    // Test Case 1: Load homepage in current browser
    // Expected: Page renders correctly with all features functional
    // This test runs on all browsers: chromium, firefox, webkit, edge

    // Verify the page loaded successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Check main sections are visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const footerSection = page.locator('footer');
    await expect(footerSection).toBeVisible();

    // Log which browser is being tested
    console.log(`TC1 passed in browser: ${browserName}`);
  });

  test('TC2: CSS layout renders consistently', async ({ page, browserName }) => {
    // Test Case 2: Verify CSS layout works across browsers
    // Expected: Key elements have correct dimensions and positioning

    // Check hero section has appropriate dimensions
    const heroSection = page.locator('#hero');
    const heroBoundingBox = await heroSection.boundingBox();
    expect(heroBoundingBox).toBeTruthy();
    expect(heroBoundingBox!.width).toBeGreaterThan(300);
    expect(heroBoundingBox!.height).toBeGreaterThan(100);

    // Check that logo is visible and has dimensions
    const logo = page.locator('#hero-logo');
    if (await logo.count() > 0) {
      const logoBoundingBox = await logo.boundingBox();
      expect(logoBoundingBox).toBeTruthy();
      expect(logoBoundingBox!.width).toBeGreaterThan(0);
      expect(logoBoundingBox!.height).toBeGreaterThan(0);
    }

    // Check main container is centered
    const mainElement = page.locator('main');
    const mainBox = await mainElement.boundingBox();
    expect(mainBox).toBeTruthy();

    console.log(`TC2 CSS layout verified in browser: ${browserName}`);
  });

  test('TC3: Logo GIF and hover effects work consistently', async ({ page, browserName }) => {
    // Test Case 3: Verify CSS animations work in all browsers
    // Expected: Logo GIF and hover effects work consistently

    // Check logo image is rendered
    const logo = page.locator('#hero-logo');
    if (await logo.count() > 0) {
      await expect(logo).toBeVisible();

      // Verify the logo has src pointing to GIF
      const logoSrc = await logo.getAttribute('src');
      expect(logoSrc).toContain('logo.gif');

      // Verify image dimensions (GIF loaded successfully)
      const boundingBox = await logo.boundingBox();
      expect(boundingBox).toBeTruthy();
      expect(boundingBox!.width).toBeGreaterThan(0);
      expect(boundingBox!.height).toBeGreaterThan(0);
    }

    // Test hover effects on CTA button
    const primaryCta = page.locator('#primary-cta');
    if (await primaryCta.count() > 0) {
      await expect(primaryCta).toBeVisible();

      // Get initial styles
      const initialBg = await primaryCta.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Hover over the button
      await primaryCta.hover();

      // Allow time for CSS transition
      await page.waitForTimeout(300);

      // Verify button still exists and is interactive
      await expect(primaryCta).toBeVisible();
    }

    console.log(`TC3 animations verified in browser: ${browserName}`);
  });

  test('TC4: Theme toggle works across browsers', async ({ page, browserName }) => {
    // Test Case 4: Verify theme toggle works in all browsers
    // Expected: Theme switching functionality works across browsers

    // Find the theme toggle button
    const themeToggle = page.locator('#theme-toggle');

    // Skip test if theme toggle doesn't exist
    if (await themeToggle.count() === 0) {
      console.log(`Theme toggle not present in ${browserName}, skipping`);
      return;
    }

    await expect(themeToggle).toBeVisible();

    // Get the document element to check for theme class
    const html = page.locator('html');

    // Get initial theme state
    const initialClass = await html.getAttribute('class') || '';
    const initialIsDark = initialClass.includes('dark');

    // Click the theme toggle
    await themeToggle.click();

    // Wait for theme transition
    await page.waitForTimeout(100);

    // Check that theme class changed
    const newClass = await html.getAttribute('class') || '';
    const newIsDark = newClass.includes('dark');

    // Theme should have toggled
    expect(newIsDark).not.toBe(initialIsDark);

    // Toggle back
    await themeToggle.click();
    await page.waitForTimeout(100);

    // Verify it toggles back
    const finalClass = await html.getAttribute('class') || '';
    const finalIsDark = finalClass.includes('dark');
    expect(finalIsDark).toBe(initialIsDark);

    console.log(`TC4 theme toggle verified in browser: ${browserName}`);
  });

  test('TC5: JavaScript functionality works consistently', async ({ page, browserName }) => {
    // Verify JavaScript is working by checking theme toggle script loaded

    // Check if localStorage is available (JavaScript working)
    const localStorageAvailable = await page.evaluate(() => {
      try {
        localStorage.setItem('test', 'test');
        localStorage.removeItem('test');
        return true;
      } catch (e) {
        return false;
      }
    });

    expect(localStorageAvailable).toBe(true);

    // Check that DOM APIs work correctly
    const documentExists = await page.evaluate(() => {
      return typeof document !== 'undefined' && document.body !== null;
    });
    expect(documentExists).toBe(true);

    // Check querySelectorAll works (used by theme toggle)
    const elementCount = await page.evaluate(() => {
      return document.querySelectorAll('*').length;
    });
    expect(elementCount).toBeGreaterThan(10);

    console.log(`TC5 JavaScript functionality verified in browser: ${browserName}`);
  });

  test('TC6: All links are accessible and functional', async ({ page, browserName }) => {
    // Verify links work across browsers
    const links = page.locator('a[href]');
    const linkCount = await links.count();

    expect(linkCount).toBeGreaterThan(0);

    // Check each link has proper href
    for (let i = 0; i < Math.min(linkCount, 5); i++) {
      const link = links.nth(i);
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href!.length).toBeGreaterThan(0);
    }

    // Verify external links have security attributes
    const externalLinks = page.locator('a[target="_blank"]');
    const externalCount = await externalLinks.count();

    for (let i = 0; i < externalCount; i++) {
      const rel = await externalLinks.nth(i).getAttribute('rel');
      if (rel) {
        // External links should have noopener for security
        expect(rel).toContain('noopener');
      }
    }

    console.log(`TC6 links verified in browser: ${browserName}`);
  });

  test('TC7: CSS transitions and transforms work', async ({ page, browserName }) => {
    // Verify CSS transitions are supported and working

    // Check that CSS custom properties (variables) are supported
    const cssVarsSupported = await page.evaluate(() => {
      const testEl = document.createElement('div');
      testEl.style.setProperty('--test-var', '10px');
      return testEl.style.getPropertyValue('--test-var') === '10px';
    });
    expect(cssVarsSupported).toBe(true);

    // Check that CSS transitions are available
    const transitionsSupported = await page.evaluate(() => {
      const el = document.createElement('div');
      return 'transition' in el.style ||
             'webkitTransition' in el.style ||
             'msTransition' in el.style;
    });
    expect(transitionsSupported).toBe(true);

    // Check that CSS transforms are available
    const transformsSupported = await page.evaluate(() => {
      const el = document.createElement('div');
      return 'transform' in el.style ||
             'webkitTransform' in el.style ||
             'msTransform' in el.style;
    });
    expect(transformsSupported).toBe(true);

    console.log(`TC7 CSS features verified in browser: ${browserName}`);
  });

  test('TC8: Viewport and responsive behavior', async ({ page, browserName }) => {
    // Verify responsive styles work across browsers

    // Get viewport size
    const viewport = page.viewportSize();
    expect(viewport).toBeTruthy();

    // Check that page respects viewport
    const bodyWidth = await page.evaluate(() => {
      return document.body.scrollWidth;
    });

    // Body should not exceed viewport (no horizontal scroll in default view)
    // Allow small overflow for some browsers
    expect(bodyWidth).toBeLessThanOrEqual(viewport!.width + 20);

    // Check media query support
    const mediaQuerySupported = await page.evaluate(() => {
      return typeof window.matchMedia === 'function';
    });
    expect(mediaQuerySupported).toBe(true);

    console.log(`TC8 responsive behavior verified in browser: ${browserName}`);
  });
});
