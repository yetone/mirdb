// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Scenario 2 - Hero Section and Value Proposition
 *
 * Test Cases:
 * 1. Hero section is present with centered logo and text content
 * 2. Headline contains 'MirDB' and mentions 'Persistent Key-Value Store' and 'Memcached Protocol'
 * 3. Subheadline explains core value proposition below the main headline
 * 4. 'Get Started' button scrolls to or navigates to the quick-start section
 */

test.describe('Hero Section and Value Proposition', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Hero section is present with centered logo and text content', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify logo is present
    const heroLogo = page.locator('[data-testid="hero-logo"]');
    await expect(heroLogo).toBeVisible();

    // Verify logo is centered by checking it has mx-auto class
    const logoClasses = await heroLogo.getAttribute('class');
    expect(logoClasses).toContain('mx-auto');

    // Verify headline is present
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();

    // Verify subheadline is present
    const subheadline = page.locator('[data-testid="hero-subheadline"]');
    await expect(subheadline).toBeVisible();

    // Verify CTA button is present
    const ctaButton = page.locator('[data-testid="hero-cta-button"]');
    await expect(ctaButton).toBeVisible();

    // Verify hero section has centered layout (text-center class on container)
    const heroContainer = heroSection.locator('.text-center');
    await expect(heroContainer).toBeVisible();
  });

  test('Test Case 2: Headline contains MirDB and mentions Persistent Key-Value Store and Memcached Protocol', async ({ page }) => {
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();

    const headlineText = await headline.textContent();

    // Verify headline contains 'MirDB'
    expect(headlineText).toContain('MirDB');

    // Verify headline mentions 'Persistent Key-Value Store'
    expect(headlineText).toContain('Persistent Key-Value Store');

    // Verify headline mentions 'Memcached Protocol'
    expect(headlineText).toContain('Memcached Protocol');
  });

  test('Test Case 3: Subheadline explains core value proposition below the main headline', async ({ page }) => {
    const subheadline = page.locator('[data-testid="hero-subheadline"]');
    await expect(subheadline).toBeVisible();

    const subheadlineText = await subheadline.textContent();

    // Verify subheadline is not empty
    expect(subheadlineText.length).toBeGreaterThan(0);

    // Verify subheadline explains value proposition (mentions key features)
    // Should mention persistence/durability, performance, or architecture
    const hasValueProposition =
      subheadlineText.toLowerCase().includes('performance') ||
      subheadlineText.toLowerCase().includes('persistence') ||
      subheadlineText.toLowerCase().includes('durability') ||
      subheadlineText.toLowerCase().includes('lsm') ||
      subheadlineText.toLowerCase().includes('sstable');

    expect(hasValueProposition).toBeTruthy();

    // Verify subheadline appears after headline in DOM
    const headline = page.locator('[data-testid="hero-headline"]');
    const headlineBoundingBox = await headline.boundingBox();
    const subheadlineBoundingBox = await subheadline.boundingBox();

    expect(subheadlineBoundingBox.y).toBeGreaterThan(headlineBoundingBox.y);
  });

  test('Test Case 4: Get Started button scrolls to or navigates to the quick-start section', async ({ page }) => {
    const ctaButton = page.locator('[data-testid="hero-cta-button"]');
    await expect(ctaButton).toBeVisible();

    // Verify button text is 'Get Started'
    const buttonText = await ctaButton.textContent();
    expect(buttonText.trim()).toBe('Get Started');

    // Verify button has href linking to quickstart section
    const href = await ctaButton.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Click the button and verify navigation
    await ctaButton.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify URL hash changed to #quickstart
    const url = page.url();
    expect(url).toContain('#quickstart');

    // Verify quickstart section is now in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });
});
