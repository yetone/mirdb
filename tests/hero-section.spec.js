// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Hero Section Display
 *
 * Verifies the hero section displays product name, tagline, and value proposition
 * prominently (REQ-1, REQ-2)
 */

test.describe('Hero Section Display', () => {

  test.beforeEach(async ({ page }) => {
    // Navigate to the landing page
    await page.goto('/');
  });

  /**
   * Test Case 1: Product name 'MirDB' is displayed prominently
   * Input: Load index.html in browser
   * Expected: Product name 'MirDB' is displayed prominently in h1 or similar heading element
   */
  test('should display product name MirDB prominently in heading', async ({ page }) => {
    // Look for h1 element containing MirDB
    const heading = page.locator('h1');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('MirDB');
  });

  /**
   * Test Case 2: Tagline is visible in hero section
   * Input: Inspect hero section content
   * Expected: Tagline 'Persistent Key-Value Store with Memcached Protocol' or equivalent is visible
   */
  test('should display tagline about persistent key-value store with Memcached protocol', async ({ page }) => {
    // Check for hero section
    const heroSection = page.locator('section.hero, .hero, header.hero, [data-testid="hero"]').first();
    await expect(heroSection).toBeVisible();

    // Check for tagline text - look for key phrases
    const pageContent = await page.textContent('body');
    expect(pageContent).toMatch(/persistent.*key-value.*store/i);
    expect(pageContent).toMatch(/memcached.*protocol/i);
  });

  /**
   * Test Case 3: Primary CTA button is present and clickable
   * Input: Check for CTA button
   * Expected: Primary CTA button (e.g., 'Get Started' or 'View on GitHub') is present and clickable
   */
  test('should have a primary CTA button that is clickable', async ({ page }) => {
    // Look for CTA button with common labels
    const ctaButton = page.locator('a.cta, button.cta, .hero a, .hero button, [data-testid="cta"]').first();
    await expect(ctaButton).toBeVisible();

    // Check that it contains one of the expected CTA texts
    const buttonText = await ctaButton.textContent();
    const validCTATexts = ['get started', 'view on github', 'github', 'documentation', 'learn more'];
    const hasValidCTA = validCTATexts.some(text => buttonText.toLowerCase().includes(text));
    expect(hasValidCTA).toBeTruthy();

    // Verify it's clickable (has href for link or is enabled for button)
    const tagName = await ctaButton.evaluate(el => el.tagName.toLowerCase());
    if (tagName === 'a') {
      const href = await ctaButton.getAttribute('href');
      expect(href).toBeTruthy();
    } else {
      await expect(ctaButton).toBeEnabled();
    }
  });

  /**
   * Test Case 4: Value proposition text is displayed
   * Input: Verify value proposition text
   * Expected: Brief description explaining Memcached compatibility with persistent storage is displayed
   */
  test('should display value proposition explaining Memcached compatibility with persistent storage', async ({ page }) => {
    // Look for description text in hero section
    const heroSection = page.locator('section.hero, .hero, header.hero, [data-testid="hero"]').first();
    await expect(heroSection).toBeVisible();

    // Check for value proposition content - should mention both memcached compatibility and persistence
    const heroText = await heroSection.textContent();

    // Value proposition should explain the unique selling point
    expect(heroText.toLowerCase()).toMatch(/memcached/i);
    expect(heroText.toLowerCase()).toMatch(/persist/i);
  });

});
