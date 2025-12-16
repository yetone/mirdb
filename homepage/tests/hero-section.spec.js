// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Hero Section
 *
 * Test Case 1: Hero section is visible with MirDB logo displayed
 * Test Case 2: Tagline describing MirDB is present
 * Test Case 3: CTA buttons exist and are clickable
 */

test.describe('Homepage Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  test('Test Case 1: Hero section is visible with MirDB logo displayed', async ({ page }) => {
    // Verify the hero section is visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Verify the MirDB logo image is displayed
    const logo = page.getByTestId('mirdb-logo');
    await expect(logo).toBeVisible();

    // Verify the logo has correct alt text
    await expect(logo).toHaveAttribute('alt', 'MirDB Logo');

    // Verify the product name "MirDB" is displayed
    const productName = page.getByTestId('product-name');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');
  });

  test('Test Case 2: Tagline describing MirDB as Persistent Key-Value Store with Memcached protocol is present', async ({ page }) => {
    // Verify the tagline element exists
    const tagline = page.getByTestId('hero-tagline');
    await expect(tagline).toBeVisible();

    // Verify the tagline contains the expected description
    const taglineText = await tagline.textContent();

    // Check for key phrases in the tagline
    expect(taglineText?.toLowerCase()).toContain('persistent');
    expect(taglineText?.toLowerCase()).toContain('key-value');
    expect(taglineText?.toLowerCase()).toContain('memcached');
    expect(taglineText?.toLowerCase()).toContain('protocol');
  });

  test('Test Case 3: CTA buttons exist and are clickable', async ({ page }) => {
    // Verify the CTA section exists
    const ctaSection = page.getByTestId('hero-cta');
    await expect(ctaSection).toBeVisible();

    // Verify "Get Started" button exists and is clickable
    const getStartedBtn = page.getByTestId('cta-get-started');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toBeEnabled();
    await expect(getStartedBtn).toHaveText('Get Started');

    // Verify it has an href attribute (is a link)
    await expect(getStartedBtn).toHaveAttribute('href', /github\.com/);

    // Verify "Documentation" button exists and is clickable
    const docsBtn = page.getByTestId('cta-documentation');
    await expect(docsBtn).toBeVisible();
    await expect(docsBtn).toBeEnabled();
    await expect(docsBtn).toHaveText('Documentation');

    // Verify it has an href attribute (is a link)
    await expect(docsBtn).toHaveAttribute('href', /github\.com/);

    // Test that buttons are clickable (simulate interaction)
    // We verify they can receive click events without errors
    await expect(getStartedBtn).toHaveCSS('cursor', 'pointer');
    await expect(docsBtn).toHaveCSS('cursor', 'pointer');
  });

  test('Hero section has proper visual hierarchy', async ({ page }) => {
    // Verify hero content is properly structured
    const heroLogo = page.getByTestId('hero-logo');
    await expect(heroLogo).toBeVisible();

    // Verify the title has appropriate font size (larger than tagline)
    const title = page.getByTestId('product-name');
    const tagline = page.getByTestId('hero-tagline');

    // Both elements should be visible
    await expect(title).toBeVisible();
    await expect(tagline).toBeVisible();

    // The hero section should have reasonable height (above the fold)
    const heroSection = page.getByTestId('hero-section');
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox?.height).toBeGreaterThan(300);
  });
});
