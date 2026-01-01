const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Product name MirDB is displayed in the hero section', async ({ page }) => {
    // Find the hero section
    const heroSection = page.locator('[data-testid="hero-section"], .hero, #hero, section:first-of-type');
    await expect(heroSection).toBeVisible();

    // Check that the product name 'MirDB' is displayed
    const productName = page.locator('h1, .product-name, [data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');
  });

  test('TC2: Tagline is displayed in the hero section', async ({ page }) => {
    // Check that the tagline is displayed
    const tagline = page.locator('.tagline, [data-testid="tagline"], .hero p, .hero .subtitle');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store with Memcached Protocol');
  });

  test('TC3: Get Started button is visible with correct styling', async ({ page }) => {
    // Check for the primary CTA button
    const getStartedButton = page.locator('a:has-text("Get Started"), button:has-text("Get Started"), [data-testid="cta-get-started"]');
    await expect(getStartedButton).toBeVisible();

    // Verify it has some visual prominence (checking it's styled as a button/link)
    const buttonBox = await getStartedButton.boundingBox();
    expect(buttonBox).not.toBeNull();
    expect(buttonBox.width).toBeGreaterThan(50);
    expect(buttonBox.height).toBeGreaterThan(20);
  });

  test('TC4: View on GitHub button is visible and links to GitHub', async ({ page }) => {
    // Check for the secondary CTA button
    const githubButton = page.locator('a:has-text("View on GitHub"), a:has-text("GitHub"), [data-testid="cta-github"]');
    await expect(githubButton).toBeVisible();

    // Verify it links to GitHub
    const href = await githubButton.getAttribute('href');
    expect(href).toMatch(/github\.com/);
  });
});
