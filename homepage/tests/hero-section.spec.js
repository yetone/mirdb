import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Product name MirDB is displayed prominently in the hero section', async ({ page }) => {
    // Load homepage and check for 'MirDB' product name in hero section
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Verify it's prominently displayed (h1 element with significant font size)
    const tagName = await productName.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('h1');
  });

  test('TC2: Tagline is visible and communicates the core value proposition', async ({ page }) => {
    // Check for tagline text communicating persistent memcached value proposition
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();

    // Verify tagline contains key value proposition elements
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('memcached');
    expect(taglineText.toLowerCase()).toContain('rust');
  });

  test('TC3: Get Started button navigates to the Getting Started section', async ({ page }) => {
    // Click 'Get Started' button in hero section
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    // Click the button
    await getStartedBtn.click();

    // Verify user is navigated to the Getting Started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeInViewport();

    // Verify URL hash changed
    await expect(page).toHaveURL(/#getting-started/);
  });

  test('TC4: View on GitHub link directs to the MirDB repository', async ({ page }) => {
    // Check 'View on GitHub' link is present
    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveText('View on GitHub');

    // Verify link href points to GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in a new tab (target="_blank")
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has security attribute for external links
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('Hero section is visible above the fold', async ({ page }) => {
    // Verify hero section is visible without scrolling
    const heroSection = page.locator('[data-testid="hero-section"]');

    // Check that hero is in the viewport on initial load (above the fold)
    await expect(heroSection).toBeInViewport();

    // Verify all key elements are visible without scrolling
    await expect(page.locator('[data-testid="product-name"]')).toBeInViewport();
    await expect(page.locator('[data-testid="tagline"]')).toBeInViewport();
    await expect(page.locator('[data-testid="get-started-btn"]')).toBeInViewport();
    await expect(page.locator('[data-testid="github-link"]')).toBeInViewport();
  });
});
