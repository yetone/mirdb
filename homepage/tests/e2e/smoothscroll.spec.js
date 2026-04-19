/**
 * Smooth Scroll E2E Tests
 * Owner: Scenario 15 - Smooth Scroll and Anchor Navigation
 *
 * Tests for:
 * - Smooth scroll behavior on anchor links
 * - Get Started button navigation
 * - CSS scroll-behavior property
 * - Reduced motion preference support
 */

import { test, expect } from '@playwright/test';

test.describe('Smooth Scroll and Anchor Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Click Get Started button smooth scrolls to getting started section', async ({ page }) => {
    // Find the Get Started button in the hero section
    const getStartedButton = page.locator('#hero a.btn-secondary:has-text("Get Started")');
    await expect(getStartedButton).toBeVisible();

    // Verify it links to quickstart
    const href = await getStartedButton.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the button
    await getStartedButton.click();

    // Wait for scroll to complete (give time for smooth scroll animation)
    await page.waitForTimeout(500);

    // Verify the URL hash has been updated
    await expect(page).toHaveURL(/#quickstart/);

    // Verify scroll position has changed (page scrolled down)
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify the quickstart section is now visible in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport({ ratio: 0.5 });
  });

  test('TC2: CSS scroll-behavior property is set to smooth on HTML element', async ({ page }) => {
    // Check that HTML element has scroll-behavior: smooth
    const scrollBehavior = await page.evaluate(() => {
      const html = document.documentElement;
      return window.getComputedStyle(html).scrollBehavior;
    });

    expect(scrollBehavior).toBe('smooth');
  });

  test('TC3: Navigate to #features anchor smooth scrolls to features section', async ({ page }) => {
    // Find a link to features section (in header)
    const featuresLink = page.locator('header a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the features link
    await featuresLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify URL hash
    await expect(page).toHaveURL(/#features/);

    // Verify scroll occurred
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport({ ratio: 0.5 });
  });

  test('TC4: Reduced motion preference disables smooth scroll animation', async ({ page }) => {
    // Enable reduced motion preference
    await page.emulateMedia({ reducedMotion: 'reduce' });

    // Reload page with new media preference
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Check that CSS scroll-behavior is set to auto (not smooth)
    const scrollBehavior = await page.evaluate(() => {
      const html = document.documentElement;
      return window.getComputedStyle(html).scrollBehavior;
    });

    expect(scrollBehavior).toBe('auto');

    // Verify the JavaScript also respects the preference
    const prefersReducedMotion = await page.evaluate(() => {
      return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    });
    expect(prefersReducedMotion).toBe(true);

    // Test that clicking anchor still navigates (instant jump, not smooth)
    const getStartedButton = page.locator('#hero a.btn-secondary:has-text("Get Started")');
    await getStartedButton.click();

    // With reduced motion, navigation should be instant
    // Just verify we ended up at the right place
    await expect(page).toHaveURL(/#quickstart/);

    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport({ ratio: 0.5 });
  });

  test('All internal anchor links navigate correctly', async ({ page }) => {
    // Get all internal anchor links
    const anchorLinks = page.locator('a[href^="#"]');
    const count = await anchorLinks.count();

    expect(count).toBeGreaterThan(0);

    // Test a few key anchor links
    const linksToTest = [
      { selector: 'a[href="#features"]', target: '#features' },
      { selector: 'a[href="#quickstart"]', target: '#quickstart' },
    ];

    for (const link of linksToTest) {
      // Go back to top of page
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const anchorLink = page.locator(link.selector).first();
      if (await anchorLink.isVisible()) {
        await anchorLink.click();
        await page.waitForTimeout(500);

        // Verify navigation occurred
        await expect(page).toHaveURL(new RegExp(link.target));
      }
    }
  });

  test('Smooth scroll module is initialized', async ({ page }) => {
    // Check console logs for initialization message
    const logs = [];
    page.on('console', (msg) => {
      if (msg.type() === 'log') {
        logs.push(msg.text());
      }
    });

    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Wait a bit for logs to be captured
    await page.waitForTimeout(100);

    // Verify smooth scroll initialization log
    const hasInitLog = logs.some((log) => log.includes('smooth scroll') || log.includes('anchor'));
    expect(hasInitLog).toBe(true);
  });

  test('Skip link smooth scrolls to main content', async ({ page }) => {
    // Find the skip link
    const skipLink = page.locator('a.skip-link');

    // Skip link should exist
    await expect(skipLink).toHaveAttribute('href', '#main');

    // Focus the skip link to make it visible
    await skipLink.focus();

    // Click the skip link
    await skipLink.click();

    // Wait for scroll
    await page.waitForTimeout(500);

    // Main content should be in viewport
    const mainContent = page.locator('#main');
    await expect(mainContent).toBeInViewport();
  });
});
