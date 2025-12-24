// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Navigation Menu E2E Tests (REQ-6)
 * Verifies the navigation menu provides easy access to all page sections
 */

test.describe('Navigation Menu', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Navigation bar is present and visible', async ({ page }) => {
    // Query for navigation bar element
    const navbar = page.locator('nav');
    await expect(navbar).toBeVisible();

    // Verify nav has proper positioning (fixed or sticky)
    const navPosition = await navbar.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.position;
    });
    expect(['fixed', 'sticky']).toContain(navPosition);
  });

  test('TC2: MirDB logo/product name is displayed in navigation', async ({ page }) => {
    // Query for logo/product name in navigation
    const navbar = page.locator('nav');
    await expect(navbar).toBeVisible();

    // Look for MirDB brand/logo in the navigation
    const navBrand = navbar.locator('.nav-brand, [class*="brand"], [class*="logo"], a').first();
    await expect(navBrand).toBeVisible();

    // Verify it contains MirDB text
    const brandText = await navBrand.textContent();
    expect(brandText?.toLowerCase()).toContain('mirdb');
  });

  test('TC3: Click Features navigation link scrolls to Features section', async ({ page }) => {
    // Find and click Features navigation link
    const featuresLink = page.locator('nav a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify Features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('TC4: Click Quick Start navigation link scrolls to Quick Start section', async ({ page }) => {
    // Find and click Quick Start navigation link
    const quickStartLink = page.locator('nav a[href="#quick-start"]');
    await expect(quickStartLink).toBeVisible();
    await quickStartLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify Quick Start section is in viewport
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });

  test('TC5: Click Commands navigation link scrolls to Commands section', async ({ page }) => {
    // Find and click Commands navigation link
    const commandsLink = page.locator('nav a[href="#commands"]');
    await expect(commandsLink).toBeVisible();
    await commandsLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify Commands section is in viewport
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeInViewport();
  });

  test('TC6: Click Configuration navigation link scrolls to Configuration section', async ({ page }) => {
    // Find and click Configuration navigation link
    const configLink = page.locator('nav a[href="#configuration"]');
    await expect(configLink).toBeVisible();
    await configLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify Configuration section is in viewport
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeInViewport();
  });

  test('TC7: Navigation bar remains visible when scrolling (sticky/fixed position)', async ({ page }) => {
    // First verify navigation is visible at top
    const navbar = page.locator('nav');
    await expect(navbar).toBeVisible();

    // Scroll down the page significantly
    await page.evaluate(() => {
      window.scrollTo(0, 1500);
    });

    // Wait for scroll to complete
    await page.waitForTimeout(300);

    // Verify navigation bar is still visible after scrolling
    await expect(navbar).toBeVisible();
    await expect(navbar).toBeInViewport();

    // Verify the nav has fixed or sticky positioning
    const navPosition = await navbar.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.position;
    });
    expect(['fixed', 'sticky']).toContain(navPosition);
  });
});
