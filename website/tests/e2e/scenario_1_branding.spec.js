// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Scenario 1: Branding and Navigation
 * Tests for verifying the homepage displays MirDB branding including logo,
 * project name, and navigation menu correctly.
 */

test.describe('Branding and Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Page loads successfully with HTTP 200 status
   * Input: Navigate to homepage URL
   * Expected: Page loads successfully with HTTP 200 status
   */
  test('TC1: Homepage loads successfully with HTTP 200 status', async ({ page }) => {
    const response = await page.goto('/');
    expect(response).not.toBeNull();
    expect(response?.status()).toBe(200);

    // Verify basic page structure is present
    await expect(page.locator('body')).toBeVisible();
    await expect(page.locator('header')).toBeVisible();
    await expect(page.locator('main')).toBeVisible();
  });

  /**
   * Test Case 2: Logo image is present with correct attributes
   * Input: Check for logo element
   * Expected: Logo image is present with alt text 'MirDB' and src containing 'logo'
   */
  test('TC2: Logo image is present with alt text MirDB and src containing logo', async ({ page }) => {
    // Find the logo image element
    const logo = page.locator('img[alt="MirDB"]');

    // Verify logo exists and is visible
    await expect(logo).toBeVisible();

    // Verify the src attribute contains 'logo'
    const src = await logo.getAttribute('src');
    expect(src).toContain('logo');

    // Verify the logo has the correct id
    const logoById = page.locator('#logo');
    await expect(logoById).toBeVisible();
  });

  /**
   * Test Case 3: Navigation menu contains all required links
   * Input: Check navigation menu items
   * Expected: Navigation contains links: Features, Quick Start, Configuration, Code (GitHub)
   */
  test('TC3: Navigation contains all required links', async ({ page }) => {
    const nav = page.locator('nav[aria-label="Main navigation"]');
    await expect(nav).toBeVisible();

    // Check for Features link
    const featuresLink = nav.locator('a[href="#features"]').first();
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toContainText('Features');

    // Check for Quick Start link
    const quickStartLink = nav.locator('a[href="#quickstart"]').first();
    await expect(quickStartLink).toBeVisible();
    await expect(quickStartLink).toContainText('Quick Start');

    // Check for Configuration link
    const configLink = nav.locator('a[href="#configuration"]').first();
    await expect(configLink).toBeVisible();
    await expect(configLink).toContainText('Configuration');

    // Check for Code (GitHub) link
    const codeLink = nav.locator('a[href="https://github.com/yetone/mirdb"]').first();
    await expect(codeLink).toBeVisible();
    await expect(codeLink).toContainText('Code');
  });

  /**
   * Test Case 4: Each navigation link navigates to correct section/URL
   * Input: Click on each navigation link
   * Expected: Each link navigates to the correct section or external URL
   */
  test('TC4: Navigation links work correctly', async ({ page, context }) => {
    // Test Features link (internal anchor)
    await page.locator('nav a[href="#features"]').first().click();
    await expect(page).toHaveURL(/#features$/);
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Test Quick Start link (internal anchor)
    await page.locator('nav a[href="#quickstart"]').first().click();
    await expect(page).toHaveURL(/#quickstart$/);
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Test Configuration link (internal anchor)
    await page.locator('nav a[href="#configuration"]').first().click();
    await expect(page).toHaveURL(/#configuration$/);
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Test Code (GitHub) link - verify it has correct href and opens in new tab
    const codeLink = page.locator('nav a[href="https://github.com/yetone/mirdb"]').first();
    await expect(codeLink).toHaveAttribute('target', '_blank');
    await expect(codeLink).toHaveAttribute('rel', /noopener/);
    const href = await codeLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  /**
   * Additional test: Verify header is fixed at the top
   */
  test('Header is fixed at the top', async ({ page }) => {
    const header = page.locator('header');
    await expect(header).toHaveClass(/fixed/);

    // Scroll down and verify header is still visible
    await page.evaluate(() => window.scrollTo(0, 500));
    await expect(header).toBeVisible();
  });

  /**
   * Additional test: Mobile menu button exists for responsive design
   */
  test('Mobile menu button exists', async ({ page }) => {
    const mobileMenuButton = page.locator('#mobile-menu-button');
    await expect(mobileMenuButton).toBeAttached();

    // Check the mobile menu exists
    const mobileMenu = page.locator('#mobile-menu');
    await expect(mobileMenu).toBeAttached();
  });
});
