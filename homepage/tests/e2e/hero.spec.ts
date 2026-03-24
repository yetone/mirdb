/**
 * Hero Section E2E Tests
 * Owner: Scenario 2 - Hero Section
 *
 * Tests:
 * - Product name 'MirDB' is prominently displayed
 * - Tagline contains 'Persistent Key-Value Store' and 'Memcached Protocol'
 * - At least 3 feature badges are visible
 * - Get Started button smooth scrolls to quick start section
 * - View on GitHub button opens GitHub repository link
 * - Hero content is readable and buttons are tappable on mobile (320px)
 */

import { test, expect, devices } from '@playwright/test';

test.describe('Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display product name MirDB prominently', async ({ page }) => {
    // Test Case 1: Product name 'MirDB' is prominently displayed
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify it's prominently displayed (visible and has reasonable size)
    const titleBox = await heroTitle.boundingBox();
    expect(titleBox).not.toBeNull();
    expect(titleBox!.width).toBeGreaterThan(100);
    expect(titleBox!.height).toBeGreaterThan(30);
  });

  test('should display tagline with Persistent Key-Value Store and Memcached Protocol', async ({ page }) => {
    // Test Case 2: Tagline contains required text
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('Persistent Key-Value Store');
    expect(taglineText).toContain('Memcached Protocol');
  });

  test('should display at least 3 feature badges', async ({ page }) => {
    // Test Case 3: At least 3 feature badges are visible
    const badges = page.locator('.hero-badges .badge');
    await expect(badges).toHaveCount(3);

    // Verify badge content
    const badgeTexts = await badges.allTextContents();
    const requiredBadges = ['Memcached', 'Persistence', 'LSM-Tree'];

    for (const required of requiredBadges) {
      const found = badgeTexts.some(text => text.includes(required));
      expect(found).toBe(true);
    }

    // Verify all badges are visible
    for (const badge of await badges.all()) {
      await expect(badge).toBeVisible();
    }
  });

  test('should smooth scroll to quick start section when clicking Get Started', async ({ page }) => {
    // Test Case 4: Get Started button scrolls to quick start section
    const getStartedBtn = page.locator('.hero-cta .btn-primary');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click Get Started
    await getStartedBtn.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify page scrolled (either scrolled down or quickstart section is in view)
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport({ ratio: 0.5 });
  });

  test('should have View on GitHub button with correct link', async ({ page }) => {
    // Test Case 5: View on GitHub button opens GitHub repository link
    const githubBtn = page.locator('.hero-cta .btn-secondary');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toHaveText('View on GitHub');

    // Verify the link points to the correct GitHub repository
    const href = await githubBtn.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in a new tab
    const target = await githubBtn.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify noopener noreferrer for security
    const rel = await githubBtn.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('should display readable content and tappable buttons on mobile (320px)', async ({ page }) => {
    // Test Case 6: Hero content is readable and buttons are tappable on mobile
    await page.setViewportSize({ width: 320, height: 568 });

    // Verify hero section is visible
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Verify product name is visible
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();

    // Verify tagline is visible
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();

    // Verify badges are visible
    const badges = page.locator('.hero-badges .badge');
    for (const badge of await badges.all()) {
      await expect(badge).toBeVisible();
    }

    // Verify buttons are tappable (have minimum 44px height for touch targets)
    const getStartedBtn = page.locator('.hero-cta .btn-primary');
    const githubBtn = page.locator('.hero-cta .btn-secondary');

    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();

    // Check button sizes are suitable for touch
    const getStartedBox = await getStartedBtn.boundingBox();
    const githubBox = await githubBtn.boundingBox();

    expect(getStartedBox).not.toBeNull();
    expect(githubBox).not.toBeNull();

    // Minimum touch target size
    expect(getStartedBox!.height).toBeGreaterThanOrEqual(44);
    expect(githubBox!.height).toBeGreaterThanOrEqual(44);

    // Buttons should fit within viewport width
    expect(getStartedBox!.width).toBeLessThanOrEqual(320);
    expect(githubBox!.width).toBeLessThanOrEqual(320);
  });

  test('should display hero section above the fold', async ({ page }) => {
    // Additional test: Hero is visible immediately without scrolling
    const hero = page.locator('.hero');
    await expect(hero).toBeInViewport();

    // Verify hero content is centered
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();
  });

  test('should have proper accessibility attributes', async ({ page }) => {
    // Accessibility: Check for proper heading hierarchy
    const h1 = page.locator('h1.hero-title');
    await expect(h1).toHaveCount(1);

    // Check buttons are keyboard accessible
    const getStartedBtn = page.locator('.hero-cta .btn-primary');
    await getStartedBtn.focus();
    await expect(getStartedBtn).toBeFocused();

    // Tab to next button
    await page.keyboard.press('Tab');
    const githubBtn = page.locator('.hero-cta .btn-secondary');
    await expect(githubBtn).toBeFocused();
  });
});
