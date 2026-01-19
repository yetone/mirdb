// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Hero Section Display and Functionality
 * Scenario: Verify that the hero section displays correctly with product name,
 * tagline, description, and call-to-action buttons as specified in REQ-1
 */

test.describe('Hero Section Display and Functionality', () => {

  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
  });

  /**
   * Test Case 1: Product logo is displayed prominently
   * Input: Load homepage and inspect hero section
   * Expected: Product logo (assets/logo.gif) is displayed prominently
   */
  test('TC1: Product logo is displayed prominently in hero section', async ({ page }) => {
    // Find the hero section
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Check that the logo image is visible and has correct source
    const logo = page.locator('[data-testid="hero-logo"]');
    await expect(logo).toBeVisible();

    // Verify the logo source contains logo.gif
    const logoSrc = await logo.getAttribute('src');
    expect(logoSrc).toContain('logo.gif');
  });

  /**
   * Test Case 2: Tagline displays correctly
   * Input: Check hero tagline text
   * Expected: Tagline displays 'Persistent Key-Value Store with Memcached Compatibility' or approved variant
   */
  test('TC2: Hero tagline displays correct text', async ({ page }) => {
    // Find the tagline element
    const tagline = page.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();

    // Get the tagline text
    const taglineText = await tagline.textContent();

    // Check for approved tagline variants
    const approvedTaglines = [
      'Persistent Key-Value Store with Memcached Compatibility',
      'Memcached with Disk Persistence, Powered by Rust',
      'Fast, Durable, Compatible: The Better Cache'
    ];

    const hasValidTagline = approvedTaglines.some(approved =>
      taglineText.toLowerCase().includes(approved.toLowerCase()) ||
      approved.toLowerCase().includes(taglineText.toLowerCase().trim())
    );

    expect(hasValidTagline || taglineText.includes('Persistent') || taglineText.includes('Memcached')).toBeTruthy();
  });

  /**
   * Test Case 3: Primary CTA button exists and is clickable
   * Input: Verify primary CTA button exists
   * Expected: Primary CTA button labeled 'Get Started' or 'View on GitHub' is visible and clickable
   */
  test('TC3: Primary CTA button is visible and clickable', async ({ page }) => {
    // Find the primary CTA button
    const primaryCTA = page.locator('[data-testid="primary-cta"]');

    await expect(primaryCTA).toBeVisible();

    // Check the button text
    const buttonText = await primaryCTA.textContent();
    const isValidPrimaryCTA =
      buttonText.toLowerCase().includes('get started') ||
      buttonText.toLowerCase().includes('view on github') ||
      buttonText.toLowerCase().includes('github');

    expect(isValidPrimaryCTA).toBeTruthy();

    // Verify it's clickable (has href or is a button)
    const href = await primaryCTA.getAttribute('href');
    const tagName = await primaryCTA.evaluate(el => el.tagName.toLowerCase());
    expect(href !== null || tagName === 'button').toBeTruthy();
  });

  /**
   * Test Case 4: Secondary CTA button exists and is clickable
   * Input: Verify secondary CTA button exists
   * Expected: Secondary CTA button labeled 'Learn More' is visible and clickable
   */
  test('TC4: Secondary CTA button (Learn More) is visible and clickable', async ({ page }) => {
    // Find the secondary CTA button
    const secondaryCTA = page.locator('[data-testid="secondary-cta"]');

    await expect(secondaryCTA).toBeVisible();

    // Check the button text
    const buttonText = await secondaryCTA.textContent();
    expect(buttonText.toLowerCase()).toContain('learn more');

    // Verify it's clickable (has href or is a button)
    const href = await secondaryCTA.getAttribute('href');
    const tagName = await secondaryCTA.evaluate(el => el.tagName.toLowerCase());
    expect(href !== null || tagName === 'button').toBeTruthy();
  });

  /**
   * Test Case 5: Primary CTA navigates correctly
   * Input: Click primary CTA button
   * Expected: User is navigated to GitHub repository or getting-started section
   */
  test('TC5: Primary CTA navigates to GitHub or getting-started section', async ({ page }) => {
    // Find the primary CTA button
    const primaryCTA = page.locator('[data-testid="primary-cta"]');

    await expect(primaryCTA).toBeVisible();

    // Get the href attribute
    const href = await primaryCTA.getAttribute('href');
    expect(href).toBeTruthy();

    // Verify the link points to GitHub or getting-started section
    const isGitHubLink = href.includes('github.com');
    const isGettingStartedLink = href.includes('#getting-started') || href.includes('#get-started');

    expect(isGitHubLink || isGettingStartedLink).toBeTruthy();
  });

  /**
   * Additional test: Hero section is visible without scrolling (above the fold)
   */
  test('Hero section is visible in first viewport without scrolling', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Check that hero section is in the viewport
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox.y).toBeGreaterThanOrEqual(0);
    expect(boundingBox.y).toBeLessThan(800); // Should be in the first viewport
  });

});
