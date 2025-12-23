// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Hero Section Display
 * Scenario: Verify that the hero section displays product name, tagline, and value proposition
 * Related Requirements: REQ-1, US-1
 */

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Product name 'MirDB' is displayed prominently in the hero section
   */
  test('TC1: Product name MirDB is displayed prominently in the hero section', async ({ page }) => {
    // Verify the hero section exists
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify the product name is displayed
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Verify the product name is rendered as an h1 for prominence
    const h1 = page.locator('h1[data-testid="product-name"]');
    await expect(h1).toBeVisible();
  });

  /**
   * Test Case 2: Tagline mentions 'persistent key-value store' and 'Memcached protocol support'
   */
  test('TC2: Tagline mentions persistent key-value store and Memcached protocol support', async ({ page }) => {
    // Get the tagline element
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    // Get tagline text and verify it contains required keywords
    const taglineText = await tagline.textContent();
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('key-value');
    expect(taglineText.toLowerCase()).toContain('memcached');
    expect(taglineText.toLowerCase()).toContain('protocol');
  });

  /**
   * Test Case 3: 'Get Started' button is visible and clickable in the hero section
   */
  test('TC3: Get Started button is visible and clickable in the hero section', async ({ page }) => {
    // Locate the Get Started button
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');

    // Verify button is visible
    await expect(getStartedBtn).toBeVisible();

    // Verify button text
    await expect(getStartedBtn).toHaveText('Get Started');

    // Verify button is enabled/clickable
    await expect(getStartedBtn).toBeEnabled();

    // Verify it's within the hero section
    const heroSection = page.locator('[data-testid="hero-section"]');
    const heroBox = await heroSection.boundingBox();
    const btnBox = await getStartedBtn.boundingBox();

    expect(btnBox.x).toBeGreaterThanOrEqual(heroBox.x);
    expect(btnBox.y).toBeGreaterThanOrEqual(heroBox.y);
    expect(btnBox.x + btnBox.width).toBeLessThanOrEqual(heroBox.x + heroBox.width);
    expect(btnBox.y + btnBox.height).toBeLessThanOrEqual(heroBox.y + heroBox.height);
  });

  /**
   * Test Case 4: 'View on GitHub' button is visible and links to GitHub repository
   */
  test('TC4: View on GitHub button is visible and links to GitHub repository', async ({ page }) => {
    // Locate the GitHub button
    const githubBtn = page.locator('[data-testid="cta-github"]');

    // Verify button is visible
    await expect(githubBtn).toBeVisible();

    // Verify button text
    await expect(githubBtn).toHaveText('View on GitHub');

    // Verify it links to GitHub
    const href = await githubBtn.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify it opens in a new tab (has target="_blank")
    const target = await githubBtn.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has noopener for security
    const rel = await githubBtn.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  /**
   * Test Case 5: Hero section with key messaging is visible in the first viewport on desktop
   */
  test('TC5: Hero section with key messaging is visible in first viewport on desktop', async ({ page }) => {
    // Set viewport to desktop size
    await page.setViewportSize({ width: 1280, height: 800 });

    // Navigate to page fresh with new viewport
    await page.goto('/');

    // Get the hero section
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Get key elements
    const productName = page.locator('[data-testid="product-name"]');
    const tagline = page.locator('[data-testid="tagline"]');
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    const githubBtn = page.locator('[data-testid="cta-github"]');

    // Verify all key elements are visible without scrolling
    await expect(productName).toBeVisible();
    await expect(tagline).toBeVisible();
    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();

    // Verify elements are within the viewport (no scrolling needed)
    const viewportHeight = 800;

    const productNameBox = await productName.boundingBox();
    const taglineBox = await tagline.boundingBox();
    const getStartedBox = await getStartedBtn.boundingBox();
    const githubBox = await githubBtn.boundingBox();

    // Check that bottom of each element is within viewport
    expect(productNameBox.y + productNameBox.height).toBeLessThan(viewportHeight);
    expect(taglineBox.y + taglineBox.height).toBeLessThan(viewportHeight);
    expect(getStartedBox.y + getStartedBox.height).toBeLessThan(viewportHeight);
    expect(githubBox.y + githubBox.height).toBeLessThan(viewportHeight);

    // Verify scroll position is at top (page hasn't been scrolled)
    const scrollY = await page.evaluate(() => window.scrollY);
    expect(scrollY).toBe(0);
  });
});
