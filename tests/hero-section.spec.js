// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Test Case 1: Hero section displays with 'MirDB' as headline
 * Input: Load index.html in browser
 * Expected: Hero section displays with 'MirDB' as headline
 */
test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  test('TC1: Hero section displays with MirDB as headline', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify MirDB is the headline
    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');
  });

  /**
   * Test Case 2: Tagline contains key phrases
   * Input: Check hero section tagline text
   * Expected: Tagline contains 'persistent key-value store' and 'memcached compatibility'
   */
  test('TC2: Tagline contains persistent key-value store and memcached compatibility', async ({ page }) => {
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();

    const taglineText = await heroTagline.textContent();

    // Check for required phrases (case-insensitive)
    expect(taglineText.toLowerCase()).toContain('persistent key-value store');
    expect(taglineText.toLowerCase()).toContain('memcached compatibility');
  });

  /**
   * Test Case 3: At least 2 CTA buttons present
   * Input: Count CTA buttons in hero section
   * Expected: At least 2 primary CTA buttons present (e.g., Get Started, View on GitHub)
   */
  test('TC3: At least 2 CTA buttons are present in hero section', async ({ page }) => {
    const heroCtaContainer = page.locator('[data-testid="hero-cta"]');
    await expect(heroCtaContainer).toBeVisible();

    // Count CTA buttons within hero section
    const ctaButtons = heroCtaContainer.locator('.btn');
    const buttonCount = await ctaButtons.count();

    expect(buttonCount).toBeGreaterThanOrEqual(2);

    // Verify Get Started button
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    // Verify GitHub button
    const githubBtn = page.locator('[data-testid="cta-github"]');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toContainText('GitHub');
  });

  /**
   * Test Case 4: Hero section loads within 2 seconds
   * Input: Verify hero section loads within 2 seconds
   * Expected: Hero content visible within 2 seconds on standard connection
   */
  test('TC4: Hero section loads within 2 seconds', async ({ page }) => {
    const startTime = Date.now();

    // Navigate to page
    await page.goto(`file://${indexPath}`);

    // Wait for hero section to be visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible({ timeout: 2000 });

    const loadTime = Date.now() - startTime;

    // Verify load time is under 2 seconds
    expect(loadTime).toBeLessThan(2000);

    // Also verify all hero content is visible
    await expect(page.locator('[data-testid="hero-title"]')).toBeVisible();
    await expect(page.locator('[data-testid="hero-tagline"]')).toBeVisible();
    await expect(page.locator('[data-testid="hero-cta"]')).toBeVisible();
  });

  /**
   * Additional test: CTA buttons are clickable and have proper links
   */
  test('CTA buttons are clickable and have proper destinations', async ({ page }) => {
    // Verify Get Started button links to quickstart section
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    const getStartedHref = await getStartedBtn.getAttribute('href');
    expect(getStartedHref).toBe('#quickstart');

    // Verify GitHub button links to external repository
    const githubBtn = page.locator('[data-testid="cta-github"]');
    const githubHref = await githubBtn.getAttribute('href');
    expect(githubHref).toContain('github.com');
    expect(githubHref).toContain('mirdb');

    // Verify GitHub button opens in new tab
    const target = await githubBtn.getAttribute('target');
    expect(target).toBe('_blank');
  });

  /**
   * Additional test: CTA buttons are keyboard accessible
   */
  test('CTA buttons are keyboard accessible', async ({ page }) => {
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    const githubBtn = page.locator('[data-testid="cta-github"]');

    // Check that buttons have role="button" for accessibility
    await expect(getStartedBtn).toHaveAttribute('role', 'button');
    await expect(githubBtn).toHaveAttribute('role', 'button');

    // Verify buttons can receive focus
    await getStartedBtn.focus();
    await expect(getStartedBtn).toBeFocused();

    // Tab to next button
    await page.keyboard.press('Tab');
    await expect(githubBtn).toBeFocused();
  });

  /**
   * Additional test: Hero section has proper semantic structure
   */
  test('Hero section has proper semantic structure', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');

    // Verify hero section has role="banner"
    await expect(heroSection).toHaveAttribute('role', 'banner');

    // Verify headline is an h1 element
    const heroTitle = page.locator('[data-testid="hero-title"]');
    const tagName = await heroTitle.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('h1');
  });
});
