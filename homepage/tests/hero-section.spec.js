// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Hero Section Display
 * Scenario ID: 1
 * Description: Verify that the hero section prominently displays the product name,
 * tagline, and primary calls-to-action as specified in REQ-1 and Story 1
 */

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('http://localhost:3000');
  });

  /**
   * Test Case 1: Product name visibility
   * Input: Load homepage and inspect hero section
   * Expected: Product name 'MirDB' is visible in h1 or prominent heading element
   */
  test('TC1: Product name MirDB is visible in h1 heading', async ({ page }) => {
    // Verify the hero section is present
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify MirDB is displayed in h1
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Verify it's an h1 element
    const tagName = await productName.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('h1');

    // Verify it's in the first viewport (no scrolling needed)
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.y).toBe(0); // Hero should be at the top
  });

  /**
   * Test Case 2: Tagline content verification
   * Input: Check tagline text content
   * Expected: Tagline mentions both 'persistence' and 'memcached' compatibility
   */
  test('TC2: Tagline mentions persistence and memcached compatibility', async ({ page }) => {
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();

    // Verify tagline mentions persistence
    expect(taglineText.toLowerCase()).toContain('persistent');

    // Verify tagline mentions memcached
    expect(taglineText.toLowerCase()).toContain('memcached');
  });

  /**
   * Test Case 3: Primary CTA button verification
   * Input: Verify primary CTA button exists
   * Expected: Button with text 'Get Started' or 'View Documentation' is present and clickable
   */
  test('TC3: Primary CTA button is present and clickable', async ({ page }) => {
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCTA).toBeVisible();

    // Verify button text is either 'Get Started' or 'View Documentation'
    const buttonText = await primaryCTA.textContent();
    const validTexts = ['get started', 'view documentation'];
    expect(validTexts).toContain(buttonText.toLowerCase().trim());

    // Verify button is clickable (has href attribute)
    const href = await primaryCTA.getAttribute('href');
    expect(href).toBeTruthy();

    // Verify it's styled as a button (has btn class)
    await expect(primaryCTA).toHaveClass(/btn/);
    await expect(primaryCTA).toHaveClass(/btn-primary/);
  });

  /**
   * Test Case 4: Secondary CTA (GitHub) button verification
   * Input: Verify secondary CTA button exists
   * Expected: Button or link with 'GitHub' reference is present
   */
  test('TC4: Secondary CTA (GitHub) button is present', async ({ page }) => {
    const githubCTA = page.locator('[data-testid="github-cta"]');
    await expect(githubCTA).toBeVisible();

    // Verify button text references GitHub
    const buttonText = await githubCTA.textContent();
    expect(buttonText.toLowerCase()).toContain('github');

    // Verify it links to GitHub
    const href = await githubCTA.getAttribute('href');
    expect(href).toContain('github');

    // Verify it's styled as a secondary button
    await expect(githubCTA).toHaveClass(/btn/);
    await expect(githubCTA).toHaveClass(/btn-secondary/);
  });

  /**
   * Additional test: CTAs are within hero section
   */
  test('CTAs are contained within the hero section', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');
    const ctaButtons = page.locator('[data-testid="cta-buttons"]');

    await expect(ctaButtons).toBeVisible();

    // Verify CTA buttons container is inside hero section
    const ctaParent = await ctaButtons.locator('xpath=../..');
    await expect(ctaParent).toHaveAttribute('data-testid', 'hero-section');
  });
});
