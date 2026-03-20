/**
 * Branding and Tech Stack Display E2E Tests
 * Owner: Scenario 8 - Branding and Tech Stack Display
 *
 * Test cases:
 * - MirDB logo displayed in hero section
 * - Logo image loads without broken image icon
 * - Page mentions Rust as the implementation language
 * - Page mentions Tokio async runtime
 * - Rust 2018 edition and stable toolchain mentioned
 */

const { test, expect } = require('@playwright/test');

test.describe('Branding and Tech Stack Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: MirDB logo displayed in hero section', async ({ page }) => {
    // Verify hero section has a logo container
    const heroLogo = page.locator('[data-testid="hero-logo"]');
    await expect(heroLogo).toBeVisible();

    // Verify logo image is present
    const logoImage = page.locator('[data-testid="mirdb-logo"]');
    await expect(logoImage).toBeVisible();

    // Verify the logo has proper alt text
    await expect(logoImage).toHaveAttribute('alt', 'MirDB Logo');
  });

  test('TC2: Logo image loads correctly without broken image icon', async ({ page }) => {
    // Find the logo image
    const logoImage = page.locator('[data-testid="mirdb-logo"]');
    await expect(logoImage).toBeVisible();

    // Verify image has loaded successfully by checking naturalWidth
    const imageLoaded = await logoImage.evaluate((img) => {
      // For SVG images, check if it has dimensions
      if (img.src.endsWith('.svg')) {
        return img.complete && img.naturalWidth > 0;
      }
      return img.complete && img.naturalWidth > 0 && img.naturalHeight > 0;
    });

    expect(imageLoaded).toBe(true);

    // Check the src attribute points to logo file
    const src = await logoImage.getAttribute('src');
    expect(src).toContain('logo');
  });

  test('TC3: Page mentions Rust as the implementation language', async ({ page }) => {
    // Check the page content for Rust mentions
    const pageContent = await page.textContent('body');

    // Verify Rust is mentioned somewhere on the page
    expect(pageContent.toLowerCase()).toContain('rust');

    // Verify it's mentioned in the context of technology/implementation
    // Check footer tech stack section
    const techStackList = page.locator('[data-testid="tech-stack-list"]');
    await expect(techStackList).toBeVisible();

    const rustBadge = page.locator('[data-testid="tech-rust"]');
    await expect(rustBadge).toBeVisible();
    await expect(rustBadge).toContainText('Rust');
  });

  test('TC4: Page mentions Tokio async runtime', async ({ page }) => {
    // Check the page content for Tokio mentions
    const pageContent = await page.textContent('body');

    // Verify Tokio is mentioned somewhere on the page
    expect(pageContent.toLowerCase()).toContain('tokio');

    // Check footer tech stack section
    const tokioBadge = page.locator('[data-testid="tech-tokio"]');
    await expect(tokioBadge).toBeVisible();
    await expect(tokioBadge).toContainText('Tokio');

    // Verify "async runtime" is mentioned
    const footerDescription = page.locator('[data-testid="footer-tech-description"]');
    const descriptionText = await footerDescription.textContent();
    expect(descriptionText.toLowerCase()).toContain('async runtime');
  });

  test('TC5: Rust 2018 edition and stable toolchain mentioned', async ({ page }) => {
    // Check for Rust 2018 edition mention
    const pageContent = await page.textContent('body');

    // Verify "Rust 2018" or "2018 edition" is mentioned
    expect(pageContent).toContain('2018');

    // Verify stable toolchain is mentioned
    expect(pageContent.toLowerCase()).toContain('stable');
    expect(pageContent.toLowerCase()).toContain('toolchain');

    // Specifically check the footer tech description
    const footerDescription = page.locator('[data-testid="footer-tech-description"]');
    await expect(footerDescription).toBeVisible();

    const descriptionText = await footerDescription.textContent();
    expect(descriptionText).toContain('2018');
    expect(descriptionText.toLowerCase()).toContain('stable toolchain');
  });

  test('Logo appears in navigation header', async ({ page }) => {
    // Verify navigation also has the MirDB branding
    const headerLogo = page.locator('.header__logo');
    await expect(headerLogo).toBeVisible();

    // Check for MirDB text in header
    const headerLogoText = page.locator('.header__logo-text');
    await expect(headerLogoText).toContainText('MirDB');
  });

  test('Logo appears in footer', async ({ page }) => {
    // Verify footer also has the MirDB branding
    const footerLogo = page.locator('.footer__logo');
    await expect(footerLogo).toBeVisible();

    // Check for MirDB text in footer
    const footerLogoText = page.locator('.footer__logo-text');
    await expect(footerLogoText).toContainText('MirDB');
  });

  test('Tech stack badges are properly styled', async ({ page }) => {
    // Check tech badges have proper styling
    const techStackList = page.locator('[data-testid="tech-stack-list"]');
    await expect(techStackList).toBeVisible();

    // Verify badges have background styling
    const rustBadge = page.locator('[data-testid="tech-rust"]');
    const backgroundColor = await rustBadge.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Should have some background color (not transparent)
    expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(backgroundColor).not.toBe('transparent');
  });
});
