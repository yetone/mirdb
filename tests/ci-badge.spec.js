// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Project Status and CI Badge', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: CircleCI build status badge is visible on the page', async ({ page }) => {
    // Check the hero section badge
    const heroBadge = page.locator('.hero-badge img[alt*="CircleCI"]');
    await expect(heroBadge).toBeVisible();

    // Verify the badge image src points to CircleCI
    await expect(heroBadge).toHaveAttribute('src', /circleci\.com\/gh\/yetone\/mirdb\.svg/);
  });

  test('TC2: Badge links to CircleCI build page for the project', async ({ page }) => {
    // Find the CircleCI badge link in the hero section
    const heroBadgeLink = page.locator('.hero-badge a[href*="circleci.com"]');
    await expect(heroBadgeLink).toBeVisible();

    // Verify the link points to the correct CircleCI project page
    await expect(heroBadgeLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');

    // Verify it opens in a new tab
    await expect(heroBadgeLink).toHaveAttribute('target', '_blank');

    // Verify it has proper security attributes
    await expect(heroBadgeLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Also check footer badge link
    const footerBadgeLink = page.locator('.footer-links a[href*="circleci.com"]');
    await expect(footerBadgeLink).toBeVisible();
    await expect(footerBadgeLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');
    await expect(footerBadgeLink).toHaveAttribute('target', '_blank');
    await expect(footerBadgeLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('TC3: Badge image has descriptive alt text indicating build status', async ({ page }) => {
    // Check hero badge alt text
    const heroBadge = page.locator('.hero-badge img');
    await expect(heroBadge).toBeVisible();

    const heroAltText = await heroBadge.getAttribute('alt');
    expect(heroAltText).toBeTruthy();
    expect(heroAltText.toLowerCase()).toContain('circleci');
    expect(heroAltText.toLowerCase()).toContain('build');
    expect(heroAltText.toLowerCase()).toContain('status');

    // Check footer badge alt text
    const footerBadge = page.locator('.footer-links img[src*="circleci"]');
    await expect(footerBadge).toBeVisible();

    const footerAltText = await footerBadge.getAttribute('alt');
    expect(footerAltText).toBeTruthy();
    expect(footerAltText.toLowerCase()).toContain('circleci');
    expect(footerAltText.toLowerCase()).toContain('build');
    expect(footerAltText.toLowerCase()).toContain('status');
  });

  test('CircleCI badge is positioned appropriately in the hero section', async ({ page }) => {
    // Verify the badge container exists in the hero section
    const heroBadgeContainer = page.locator('.hero-badge');
    await expect(heroBadgeContainer).toBeVisible();

    // Verify it's below the CTA buttons (proper visual hierarchy)
    const ctaButtons = page.locator('.hero-ctas');
    const ctaBoundingBox = await ctaButtons.boundingBox();
    const badgeBoundingBox = await heroBadgeContainer.boundingBox();

    expect(ctaBoundingBox).not.toBeNull();
    expect(badgeBoundingBox).not.toBeNull();

    // Badge should be below the CTA buttons
    expect(badgeBoundingBox.y).toBeGreaterThan(ctaBoundingBox.y);
  });

  test('Footer CircleCI badge is visible when scrolled to bottom', async ({ page }) => {
    // Scroll to footer
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

    // Find the CircleCI badge in footer
    const footerBadge = page.locator('.footer-links img[src*="circleci"]');
    await expect(footerBadge).toBeVisible();

    // Verify the link wrapping the badge
    const footerBadgeLink = page.locator('.footer-links a[href*="circleci.com"]');
    await expect(footerBadgeLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');
  });
});
