import { test, expect } from '@playwright/test';

/**
 * User Value Understanding Tests
 *
 * Scenario: Verify users can understand MirDB value proposition within 10 seconds
 *
 * This test suite validates that users landing on the homepage can quickly grasp:
 * 1. The product name (MirDB)
 * 2. The core value proposition (persistent memcached)
 * 3. The target audience (developers working with key-value stores)
 */
test.describe('User Value Understanding - Above the Fold', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Product name 'MirDB' and tagline are visible without scrolling
   * Type: E2E
   */
  test('TC1: Product name MirDB and tagline visible without scrolling', async ({ page }) => {
    // Verify we haven't scrolled - page should be at top
    const scrollY = await page.evaluate(() => window.scrollY);
    expect(scrollY).toBe(0);

    // Check hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();
    await expect(heroSection).toBeInViewport();

    // Check product name "MirDB" is visible without scrolling
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toBeInViewport();
    await expect(productName).toHaveText('MirDB');

    // Verify product name is an H1 element (prominently displayed)
    const tagName = await productName.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('h1');

    // Check tagline is visible without scrolling
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toBeInViewport();

    // Verify tagline text is present and communicates the value proposition
    const taglineText = await tagline.textContent();
    expect(taglineText).toBeTruthy();
    expect(taglineText!.length).toBeGreaterThan(0);
  });

  /**
   * Test Case 2: Value proposition (persistent memcached) is communicated within the hero section text
   * Type: Manual (automated validation of content)
   */
  test('TC2: Value proposition communicates persistent memcached in hero section', async ({ page }) => {
    // Get the hero section
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Get the tagline element
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();
    expect(taglineText).toBeTruthy();

    // Verify tagline communicates the core value proposition
    const taglineLower = taglineText!.toLowerCase();
    expect(taglineLower).toContain('persistent');
    expect(taglineLower).toContain('memcached');

    // Also check for Rust mention as a key differentiator
    expect(taglineLower).toContain('rust');

    // Verify tagline is concise and readable quickly (under 100 characters)
    expect(taglineText!.length).toBeLessThan(100);

    // Check for additional description that reinforces the value proposition
    const heroDescription = heroSection.locator('.hero-description');
    await expect(heroDescription).toBeVisible();

    const descriptionText = await heroDescription.textContent();
    expect(descriptionText).toBeTruthy();

    // Description should mention key concepts
    const descLower = descriptionText!.toLowerCase();
    expect(descLower).toContain('key-value');
    expect(descLower).toContain('memcached');
    expect(descLower).toContain('persistent') || expect(descLower).toContain('durable');
  });

  /**
   * Test Case 3: Content makes clear this is for developers working with key-value stores
   * Type: Manual (automated validation of developer-targeted content)
   */
  test('TC3: Target audience identified as developers working with key-value stores', async ({ page }) => {
    // Get the hero section content
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Collect all text content from the hero section
    const heroText = await heroSection.textContent();
    expect(heroText).toBeTruthy();
    const heroTextLower = heroText!.toLowerCase();

    // Verify developer-oriented terminology is present
    // Should mention "key-value" which is developer terminology
    expect(heroTextLower).toContain('key-value');

    // Should have technical terms that developers would recognize
    const hasTechnicalTerms =
      heroTextLower.includes('protocol') ||
      heroTextLower.includes('store') ||
      heroTextLower.includes('drop-in') ||
      heroTextLower.includes('replacement');
    expect(hasTechnicalTerms).toBe(true);

    // Verify Call-to-Action buttons are developer-friendly
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveText('View on GitHub');

    // GitHub link indicates this is for developers
    const githubHref = await githubLink.getAttribute('href');
    expect(githubHref).toContain('github.com');
  });

  /**
   * Additional validation: Hero section loads quickly (within performance budget)
   */
  test('Hero section content is immediately visible on page load', async ({ page }) => {
    // Navigate and wait for DOM content loaded
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Product name should be immediately visible
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible({ timeout: 1000 });

    // Tagline should be immediately visible
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible({ timeout: 1000 });

    // Both CTAs should be immediately visible
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await expect(getStartedBtn).toBeVisible({ timeout: 1000 });

    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible({ timeout: 1000 });
  });

  /**
   * Validate that the hero section provides a complete first impression
   */
  test('Hero section provides complete first impression without scrolling', async ({ page }) => {
    // Get viewport height
    const viewportHeight = await page.evaluate(() => window.innerHeight);

    // Get hero section bounding box
    const heroSection = page.locator('[data-testid="hero-section"]');
    const heroBoundingBox = await heroSection.boundingBox();

    expect(heroBoundingBox).toBeTruthy();

    // Verify hero section starts at or near the top of the page
    // (accounting for navigation bar)
    expect(heroBoundingBox!.y).toBeLessThan(100);

    // Get all key elements' positions
    const productName = page.locator('[data-testid="product-name"]');
    const tagline = page.locator('[data-testid="tagline"]');
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');

    const productNameBox = await productName.boundingBox();
    const taglineBox = await tagline.boundingBox();
    const getStartedBox = await getStartedBtn.boundingBox();

    expect(productNameBox).toBeTruthy();
    expect(taglineBox).toBeTruthy();
    expect(getStartedBox).toBeTruthy();

    // All key elements should be within the first viewport
    expect(productNameBox!.y + productNameBox!.height).toBeLessThan(viewportHeight);
    expect(taglineBox!.y + taglineBox!.height).toBeLessThan(viewportHeight);
    expect(getStartedBox!.y + getStartedBox!.height).toBeLessThan(viewportHeight);
  });
});
