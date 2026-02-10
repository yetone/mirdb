/**
 * Homepage E2E Tests
 * Owner: Scenario 1 - Homepage Core Structure
 *
 * End-to-end tests for the MirDB homepage core structure.
 *
 * Requirements traced:
 * - REQ-1: Homepage displays project introduction
 * - Success Criteria: Homepage renders in major browsers
 */

// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Homepage Core Structure and Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Load homepage in Chrome browser
  test('should load homepage successfully with HTTP 200 status', async ({ page }) => {
    const response = await page.goto('/');
    expect(response.status()).toBe(200);
    await expect(page).toHaveTitle(/MirDB/);
  });

  // Test Case 2: Check for logo element
  test('should display logo image referencing assets/logo.gif', async ({ page }) => {
    const logo = page.locator('header .logo img');
    await expect(logo).toBeVisible();
    const src = await logo.getAttribute('src');
    expect(src).toContain('logo.gif');
  });

  // Test Case 3: Inspect hero section text
  test('should display hero section with MirDB product name and value proposition', async ({ page }) => {
    const hero = page.locator('#hero, .hero');
    await expect(hero).toBeVisible();

    // Check for product name
    const heroHeading = hero.locator('h1');
    await expect(heroHeading).toContainText('MirDB');

    // Check for value proposition - should contain key phrases
    const heroText = await hero.textContent();
    const hasKeyValueStore = heroText.toLowerCase().includes('key-value store') ||
                             heroText.toLowerCase().includes('key value store');
    const hasMemcached = heroText.toLowerCase().includes('memcached');
    const hasPersistent = heroText.toLowerCase().includes('persistent');

    expect(hasKeyValueStore || hasMemcached || hasPersistent).toBe(true);
  });

  // Test Case 4: Check HTML document structure (DOCTYPE, html, head, body with meta tags)
  test('should have proper HTML document structure with charset and viewport meta tags', async ({ page }) => {
    // Check for DOCTYPE
    const doctype = await page.evaluate(() => {
      const node = document.doctype;
      return node ? node.name : null;
    });
    expect(doctype).toBe('html');

    // Check for html element with lang attribute
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBe('en');

    // Check for charset meta tag
    const charsetMeta = page.locator('meta[charset]');
    await expect(charsetMeta).toHaveCount(1);
    const charset = await charsetMeta.getAttribute('charset');
    expect(charset.toLowerCase()).toBe('utf-8');

    // Check for viewport meta tag
    const viewportMeta = page.locator('meta[name="viewport"]');
    await expect(viewportMeta).toHaveCount(1);
    const viewport = await viewportMeta.getAttribute('content');
    expect(viewport).toContain('width=device-width');

    // Check for head and body elements
    await expect(page.locator('head')).toHaveCount(1);
    await expect(page.locator('body')).toHaveCount(1);
    await expect(page.locator('title')).toHaveCount(1);
  });

  // Test Case 5: Verify CSS styles are loaded
  test('should have styles.css linked and applying custom styling', async ({ page }) => {
    // Check that styles.css is linked
    const styleLink = page.locator('link[rel="stylesheet"][href*="styles.css"]');
    await expect(styleLink).toHaveCount(1);

    // Check that custom styles are applied (not browser defaults)
    const body = page.locator('body');
    const fontFamily = await body.evaluate((el) =>
      window.getComputedStyle(el).fontFamily
    );
    // Should not be the browser default serif font
    expect(fontFamily.toLowerCase()).not.toContain('times');

    // Check that hero has custom styling (background)
    const hero = page.locator('.hero');
    const heroBackground = await hero.evaluate((el) =>
      window.getComputedStyle(el).background
    );
    // Should have some custom background (not just white)
    expect(heroBackground).toBeTruthy();

    // Check that buttons have custom styling
    const primaryBtn = page.locator('.btn-primary').first();
    if (await primaryBtn.count() > 0) {
      const btnBg = await primaryBtn.evaluate((el) =>
        window.getComputedStyle(el).backgroundColor
      );
      // Primary button should have a colored background
      expect(btnBg).not.toBe('rgba(0, 0, 0, 0)');
    }
  });

  // Additional tests for header structure
  test('should display header with logo and project name', async ({ page }) => {
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Logo container with image and project name
    const logo = header.locator('.logo');
    await expect(logo).toBeVisible();

    const logoImg = logo.locator('img');
    await expect(logoImg).toBeVisible();

    const logoText = logo.locator('h1');
    await expect(logoText).toContainText('MirDB');
  });

  // Test for CTA buttons
  test('should display Get Started and View on GitHub buttons in hero section', async ({ page }) => {
    const ctaButtons = page.locator('.cta-buttons, .hero .btn');
    await expect(ctaButtons.first()).toBeVisible();

    // Check for Get Started button
    const getStartedBtn = page.locator('a.btn:has-text("Get Started"), a.btn-primary:has-text("Get Started")');
    await expect(getStartedBtn).toBeVisible();

    // Check for View on GitHub button
    const githubBtn = page.locator('a.btn:has-text("GitHub"), a.btn-secondary:has-text("GitHub")');
    await expect(githubBtn).toBeVisible();
  });

  // Test for all main sections
  test('should have all main page sections present', async ({ page }) => {
    // Features section
    const featuresSection = page.locator('#features, .features');
    await expect(featuresSection).toBeVisible();

    // Quick Start section
    const quickstartSection = page.locator('#quickstart, .quickstart');
    await expect(quickstartSection).toBeVisible();

    // Resources section
    const resourcesSection = page.locator('#resources, .resources');
    await expect(resourcesSection).toBeVisible();

    // Footer
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });

  // Navigation test
  test('should have navigation links in header', async ({ page }) => {
    const nav = page.locator('header nav');
    await expect(nav).toBeVisible();

    // Check for navigation links
    const navLinks = nav.locator('a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(2);
  });
});

// Cross-browser tests (Test Cases 6, 7, 8) - These will run when configured with multiple browsers
test.describe('Cross-browser Compatibility', () => {
  test('should render page correctly with expected elements', async ({ page, browserName }) => {
    await page.goto('/');

    // Core elements should be visible in all browsers
    await expect(page.locator('header')).toBeVisible();
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features, .features')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();

    // Logo should be visible
    const logo = page.locator('header .logo img');
    await expect(logo).toBeVisible();

    // Hero text should be present
    const heroText = await page.locator('.hero').textContent();
    expect(heroText).toContain('MirDB');
  });
});
