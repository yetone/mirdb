/**
 * Navigation E2E Tests
 * Owner: Scenario 6 - Navigation and Anchor Links
 *
 * Tests:
 * - Navigation menu presence
 * - All navigation links present
 * - Smooth scroll to sections
 * - Keyboard navigation support
 */

const { test, expect } = require('@playwright/test');
const { waitForPageLoad } = require('../test-utils/helpers');

test.describe('Navigation and Anchor Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('TC1: Header contains navigation with links to Features, Quick Start, Usage, Status sections', async ({ page }) => {
    // Check header and nav presence
    const header = page.locator('header');
    await expect(header).toBeVisible();

    const nav = page.locator('nav#main-nav');
    await expect(nav).toBeVisible();

    // Check for all required navigation links
    const navMenu = page.locator('.nav-menu');
    await expect(navMenu).toBeVisible();

    // Features link
    const featuresLink = page.locator('.nav-link[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveText('Features');

    // Quick Start link
    const quickStartLink = page.locator('.nav-link[href="#quick-start"]');
    await expect(quickStartLink).toBeVisible();
    await expect(quickStartLink).toHaveText('Quick Start');

    // Usage link
    const usageLink = page.locator('.nav-link[href="#usage"]');
    await expect(usageLink).toBeVisible();
    await expect(usageLink).toHaveText('Usage');

    // Status link
    const statusLink = page.locator('.nav-link[href="#status"]');
    await expect(statusLink).toBeVisible();
    await expect(statusLink).toHaveText('Status');

    // Verify all four links are present
    const navLinks = page.locator('.nav-link');
    await expect(navLinks).toHaveCount(4);
  });

  test('TC2: Click Features nav link - Page smoothly scrolls to Features section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Features nav link
    await page.click('.nav-link[href="#features"]');

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(500);

    // Check that Features section is now in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify scroll position changed (scrolled down)
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);
  });

  test('TC3: Click Quick Start nav link - Page smoothly scrolls to Quick Start section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Quick Start nav link
    await page.click('.nav-link[href="#quick-start"]');

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(500);

    // Check that Quick Start section is now in view
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();

    // Verify scroll position changed
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);
  });

  test('TC4: Click Usage nav link - Page smoothly scrolls to Usage section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Usage nav link
    await page.click('.nav-link[href="#usage"]');

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(500);

    // Check that Usage section is now in view
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeInViewport();

    // Verify scroll position changed
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);
  });

  test('TC5: Click Status nav link - Page smoothly scrolls to Status section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Status nav link
    await page.click('.nav-link[href="#status"]');

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(500);

    // Check that Status section is now in view
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeInViewport();

    // Verify scroll position changed
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);
  });

  test('TC6: Tab through navigation - All navigation items receive focus in order and can be activated with Enter key', async ({ page }) => {
    // Start by focusing on the logo
    await page.focus('.logo');

    // Tab to first nav link (Features)
    await page.keyboard.press('Tab');
    let focusedElement = await page.evaluate(() => document.activeElement?.textContent);
    expect(focusedElement).toBe('Features');

    // Verify focus indicator is visible
    const featuresLink = page.locator('.nav-link[href="#features"]');
    await expect(featuresLink).toBeFocused();

    // Tab to second nav link (Quick Start)
    await page.keyboard.press('Tab');
    focusedElement = await page.evaluate(() => document.activeElement?.textContent);
    expect(focusedElement).toBe('Quick Start');

    const quickStartLink = page.locator('.nav-link[href="#quick-start"]');
    await expect(quickStartLink).toBeFocused();

    // Tab to third nav link (Usage)
    await page.keyboard.press('Tab');
    focusedElement = await page.evaluate(() => document.activeElement?.textContent);
    expect(focusedElement).toBe('Usage');

    const usageLink = page.locator('.nav-link[href="#usage"]');
    await expect(usageLink).toBeFocused();

    // Tab to fourth nav link (Status)
    await page.keyboard.press('Tab');
    focusedElement = await page.evaluate(() => document.activeElement?.textContent);
    expect(focusedElement).toBe('Status');

    const statusLink = page.locator('.nav-link[href="#status"]');
    await expect(statusLink).toBeFocused();

    // Test activation with Enter key
    // Go back to Features link
    await page.focus('.nav-link[href="#features"]');
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Press Enter to activate the link
    await page.keyboard.press('Enter');

    // Wait for smooth scroll
    await page.waitForTimeout(500);

    // Verify scroll occurred
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify Features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Smooth scroll behavior is enabled via CSS', async ({ page }) => {
    // Check that html has scroll-behavior: smooth
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(scrollBehavior).toBe('smooth');
  });

  test('Navigation has proper ARIA attributes', async ({ page }) => {
    const nav = page.locator('nav#main-nav');
    await expect(nav).toHaveAttribute('aria-label', 'Main navigation');

    const navMenu = page.locator('.nav-menu');
    await expect(navMenu).toHaveAttribute('role', 'list');
  });

  test('Navigation links have visible focus indicators', async ({ page }) => {
    // Focus on a nav link
    const featuresLink = page.locator('.nav-link[href="#features"]');
    await featuresLink.focus();

    // Get the outline style
    const outlineStyle = await page.evaluate(() => {
      const element = document.querySelector('.nav-link[href="#features"]');
      const style = window.getComputedStyle(element);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth,
        outlineColor: style.outlineColor,
        outlineStyle: style.outlineStyle
      };
    });

    // Verify focus indicator exists (outline should be present)
    expect(outlineStyle.outlineWidth).not.toBe('0px');
  });

  test('Header remains sticky at top during scroll', async ({ page }) => {
    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(100);

    // Check header is still visible at top
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Check header position
    const headerPosition = await page.evaluate(() => {
      const header = document.querySelector('header');
      const style = window.getComputedStyle(header);
      return style.position;
    });
    expect(headerPosition).toBe('sticky');
  });
});
