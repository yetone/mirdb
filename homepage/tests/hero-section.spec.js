// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Helper to get the file URL for the homepage
const getHomepageUrl = () => {
  return 'file://' + path.join(__dirname, '..', 'index.html');
};

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getHomepageUrl());
  });

  /**
   * Test Case 1: Hero section contains 'MirDB' as headline
   * Input: Load homepage and inspect hero section
   * Expected: Hero section contains 'MirDB' as headline
   */
  test('should display MirDB as the headline in the hero section', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify the headline contains 'MirDB'
    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify it's an h1 element
    await expect(heroTitle).toHaveAttribute('class', /hero-title/);
  });

  /**
   * Test Case 2: Tagline mentions 'Persistent Key-Value Store' and 'Memcached Protocol'
   * Input: Check tagline text
   * Expected: Tagline mentions 'Persistent Key-Value Store' and 'Memcached Protocol'
   */
  test('should display tagline with Persistent Key-Value Store and Memcached Protocol', async ({ page }) => {
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();

    const taglineText = await heroTagline.textContent();

    // Verify tagline mentions both key phrases
    expect(taglineText).toContain('Persistent Key-Value Store');
    expect(taglineText).toContain('Memcached Protocol');
  });

  /**
   * Test Case 3: Description mentions drop-in replacement for memcached with durable storage
   * Input: Check value proposition description
   * Expected: Description mentions drop-in replacement for memcached with durable storage
   */
  test('should display value proposition about drop-in replacement with durable storage', async ({ page }) => {
    const heroDescription = page.locator('[data-testid="hero-description"]');
    await expect(heroDescription).toBeVisible();

    const descriptionText = await heroDescription.textContent();

    // Verify description mentions drop-in replacement
    expect(descriptionText?.toLowerCase()).toContain('drop-in replacement');

    // Verify description mentions durable storage (or similar concept)
    expect(descriptionText?.toLowerCase()).toContain('durable');
  });

  /**
   * Test Case 4: Get Started button is visible and has correct styling
   * Input: Verify primary CTA button exists
   * Expected: Get Started button is visible and has correct styling
   */
  test('should display Get Started button as primary CTA with correct styling', async ({ page }) => {
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedBtn).toBeVisible();

    // Verify button text
    await expect(getStartedBtn).toHaveText('Get Started');

    // Verify it has the primary button styling
    await expect(getStartedBtn).toHaveClass(/btn-primary/);

    // Verify it's a clickable link
    await expect(getStartedBtn).toHaveAttribute('href', '#getting-started');
  });

  /**
   * Test Case 5: View on GitHub button/link is visible
   * Input: Verify secondary CTA button exists
   * Expected: View on GitHub button/link is visible
   */
  test('should display View on GitHub button as secondary CTA', async ({ page }) => {
    const githubBtn = page.locator('[data-testid="cta-github"]');
    await expect(githubBtn).toBeVisible();

    // Verify button text
    await expect(githubBtn).toHaveText('View on GitHub');

    // Verify it has the secondary button styling
    await expect(githubBtn).toHaveClass(/btn-secondary/);

    // Verify it links to GitHub (external link)
    await expect(githubBtn).toHaveAttribute('href', /github\.com/);

    // Verify proper external link attributes
    await expect(githubBtn).toHaveAttribute('target', '_blank');
    await expect(githubBtn).toHaveAttribute('rel', /noopener/);
  });
});
