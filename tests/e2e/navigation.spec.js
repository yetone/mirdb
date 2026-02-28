/**
 * Navigation E2E Tests
 * Owner: Scenarios 4, 14, 16
 *
 * Test cases:
 * - Navigation menu presence
 * - Internal link smooth scrolling
 * - External links open in new tab with rel=noopener
 * - Mobile hamburger menu toggle
 * - GitHub and CircleCI link validation
 * - Keyboard navigation through links
 */

const { test, expect } = require('@playwright/test');
const { VIEWPORTS, waitForPageLoad } = require('./test-utils');

test.describe('Navigation Menu Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('Test Case 1: Navigation element is present in header', async ({ page }) => {
    // Check that header exists
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Check that nav element exists with aria-label
    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();
    await expect(nav).toHaveAttribute('aria-label', 'Main navigation');

    // Check that navigation menu has links
    const navMenu = page.locator('.nav-menu');
    await expect(navMenu).toBeVisible();

    // Verify all expected links are present
    const featuresLink = page.locator('.nav-menu a[href="#features"]');
    const quickstartLink = page.locator('.nav-menu a[href="#quickstart"]');
    const statusLink = page.locator('.nav-menu a[href="#status"]');

    await expect(featuresLink).toBeVisible();
    await expect(quickstartLink).toBeVisible();
    await expect(statusLink).toBeVisible();
  });

  test('Test Case 2: Clicking Features link scrolls to features section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Features link
    const featuresLink = page.locator('.nav-menu a[href="#features"]');
    await featuresLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Check that the page has scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Check that the features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Test Case 3: GitHub link opens in new tab with correct attributes', async ({ page }) => {
    // Find the GitHub link
    const githubLink = page.locator('.nav-menu a[href*="github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Check target="_blank" attribute
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Check rel="noopener noreferrer" attribute
    const relAttr = await githubLink.getAttribute('rel');
    expect(relAttr).toContain('noopener');

    // Check for external link indicator
    const externalIcon = githubLink.locator('.external-icon');
    await expect(externalIcon).toBeVisible();
  });

  test('Test Case 4: Navigation collapses to hamburger menu on mobile viewport', async ({ page }) => {
    // Set viewport to mobile width and reload to ensure CSS media queries are applied
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.reload();
    await waitForPageLoad(page);

    // Check that hamburger button is visible
    const hamburger = page.locator('.nav-toggle');
    await expect(hamburger).toBeVisible();

    // Check that the hamburger toggle has display: flex (indicating it's shown on mobile)
    const toggleDisplay = await hamburger.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(toggleDisplay).toBe('flex');

    // Check that nav menu has hidden visibility
    const navMenu = page.locator('.nav-menu');
    const menuStyles = await navMenu.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        visibility: styles.visibility,
        opacity: styles.opacity,
        transform: styles.transform
      };
    });
    // Nav menu should be hidden (visibility: hidden OR opacity: 0)
    expect(menuStyles.visibility === 'hidden' || menuStyles.opacity === '0').toBe(true);
  });

  test('Test Case 5: Mobile hamburger menu expands when clicked', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForTimeout(100);

    // Get hamburger button and click it
    const hamburger = page.locator('.nav-toggle');
    await expect(hamburger).toBeVisible();
    await hamburger.click();

    // Wait for animation
    await page.waitForTimeout(300);

    // Check that aria-expanded is true
    await expect(hamburger).toHaveAttribute('aria-expanded', 'true');

    // Check that nav menu is now visible
    const navMenu = page.locator('.nav-menu');
    const isVisible = await navMenu.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.visibility === 'visible' && styles.opacity === '1';
    });
    expect(isVisible).toBe(true);

    // Verify all navigation links are visible
    const featuresLink = page.locator('.nav-menu a[href="#features"]');
    await expect(featuresLink).toBeVisible();
  });

  test('Test Case 6: Keyboard navigation through all links with visible focus indicators', async ({ page }) => {
    // Start by focusing the skip link
    await page.keyboard.press('Tab');

    // Tab through navigation links and check focus
    const navLinks = page.locator('.nav-menu a');
    const linkCount = await navLinks.count();

    // Tab to nav brand first
    await page.keyboard.press('Tab');

    // Then tab through each nav menu link
    for (let i = 0; i < linkCount; i++) {
      await page.keyboard.press('Tab');

      // Get the currently focused element
      const focusedElement = page.locator(':focus');

      // Check that the focused element has a visible focus indicator
      const outlineStyle = await focusedElement.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          outlineColor: styles.outlineColor,
        };
      });

      // Focus indicator should not be 'none' or '0px'
      expect(outlineStyle.outlineStyle).not.toBe('none');
    }
  });

  test('Quick Start link scrolls to quickstart section', async ({ page }) => {
    // Click the Quick Start link
    const quickstartLink = page.locator('.nav-menu a[href="#quickstart"]');
    await quickstartLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Check that the quickstart section is in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('Status link scrolls to status section', async ({ page }) => {
    // Click the Status link
    const statusLink = page.locator('.nav-menu a[href="#status"]');
    await statusLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Check that the status section is in view
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeInViewport();
  });

  test('Documentation link has correct external link attributes', async ({ page }) => {
    // Find the Docs link
    const docsLink = page.locator('.nav-menu a[href*="memcached"]');
    await expect(docsLink).toBeVisible();

    // Check target="_blank" attribute
    await expect(docsLink).toHaveAttribute('target', '_blank');

    // Check rel="noopener noreferrer" attribute
    const relAttr = await docsLink.getAttribute('rel');
    expect(relAttr).toContain('noopener');
  });

  test('Mobile menu closes when escape key is pressed', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForTimeout(100);

    // Open the menu
    const hamburger = page.locator('.nav-toggle');
    await hamburger.click();
    await page.waitForTimeout(300);

    // Verify menu is open
    await expect(hamburger).toHaveAttribute('aria-expanded', 'true');

    // Press Escape
    await page.keyboard.press('Escape');
    await page.waitForTimeout(300);

    // Verify menu is closed
    await expect(hamburger).toHaveAttribute('aria-expanded', 'false');
  });

  test('Mobile menu closes when a link is clicked', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForTimeout(100);

    // Open the menu
    const hamburger = page.locator('.nav-toggle');
    await hamburger.click();
    await page.waitForTimeout(300);

    // Click a link
    const featuresLink = page.locator('.nav-menu a[href="#features"]');
    await featuresLink.click();
    await page.waitForTimeout(300);

    // Verify menu is closed
    await expect(hamburger).toHaveAttribute('aria-expanded', 'false');
  });

  test('Nav brand link exists and links to hero', async ({ page }) => {
    const brandLink = page.locator('.nav-brand a');
    await expect(brandLink).toBeVisible();
    await expect(brandLink).toHaveAttribute('href', '#hero');
    await expect(brandLink).toHaveText('MirDB');
  });

  test('Header has fixed position', async ({ page }) => {
    const header = page.locator('header.header');
    const position = await header.evaluate((el) => {
      return window.getComputedStyle(el).position;
    });
    expect(position).toBe('fixed');
  });
});
