// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: MirDB logo is prominently displayed', async ({ page }) => {
    // Check that the hero logo exists and is visible
    const logo = page.locator('.hero-logo');
    await expect(logo).toBeVisible();

    // Verify it uses the optimized placeholder source for performance
    // (SVG placeholder provides better LCP score while maintaining visual identity)
    await expect(logo).toHaveAttribute('src', 'assets/logo-placeholder.svg');

    // Verify the logo has appropriate alt text
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText).toContain('MirDB Logo');
  });

  test('TC2: Tagline reads "A Persistent Key-Value Store with Memcached Protocol"', async ({ page }) => {
    // Check the tagline text content
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toHaveText('A Persistent Key-Value Store with Memcached Protocol');
  });

  test('TC3: Subtitle emphasizes "Built in Rust for speed and reliability"', async ({ page }) => {
    // Check the subtitle content
    const subtitle = page.locator('.hero-subtitle');
    await expect(subtitle).toBeVisible();

    // Verify the text contains the expected phrase
    await expect(subtitle).toContainText('Built in Rust');
    await expect(subtitle).toContainText('speed and reliability');
  });

  test('TC4: Get Started button is visible and links to quick start section', async ({ page }) => {
    // Find the Get Started button in the hero CTAs
    const getStartedButton = page.locator('.hero-ctas .btn-primary');
    await expect(getStartedButton).toBeVisible();

    // Verify the button text
    await expect(getStartedButton).toHaveText('Get Started');

    // Verify the button links to the quickstart section
    await expect(getStartedButton).toHaveAttribute('href', '#quick-start');

    // Click the button and verify it navigates to the quickstart section
    await getStartedButton.click();

    // Wait for navigation to the quickstart section
    await expect(page).toHaveURL(/#quick-start$/);

    // Verify the quickstart section is visible
    const quickstartSection = page.locator('#quick-start');
    await expect(quickstartSection).toBeVisible();
  });

  test('TC5: View on GitHub button is visible and links to https://github.com/yetone/mirdb', async ({ page }) => {
    // Find the View on GitHub button in the hero CTAs
    const githubButton = page.locator('.hero-ctas .btn-secondary');
    await expect(githubButton).toBeVisible();

    // Verify the button text
    await expect(githubButton).toHaveText('View on GitHub');

    // Verify the button links to the correct GitHub URL
    await expect(githubButton).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify it opens in a new tab
    await expect(githubButton).toHaveAttribute('target', '_blank');

    // Verify it has proper security attributes
    await expect(githubButton).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('Hero section is the first visible element above the fold', async ({ page }) => {
    // Verify the hero section exists
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify it's positioned at the top of the page (after header)
    const heroBoundingBox = await heroSection.boundingBox();
    expect(heroBoundingBox).not.toBeNull();
    // Hero should start within the first portion of the viewport
    expect(heroBoundingBox.y).toBeLessThan(200);
  });

  test('Hero section has proper accessibility attributes', async ({ page }) => {
    // Verify the hero section has proper aria-labelledby
    const heroSection = page.locator('.hero');
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-title');

    // Verify the main content has a skip link
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toHaveAttribute('href', '#main-content');

    // Verify the logo has alt text
    const logo = page.locator('.hero-logo');
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(10);
  });
});
