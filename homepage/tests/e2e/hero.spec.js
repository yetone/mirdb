/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests for:
 * - Logo display
 * - Value proposition text
 * - CTA button functionality
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Logo is visible within the hero section viewport', async ({ page }) => {
    // Navigate to homepage and check for logo element
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check logo image is visible
    const logo = heroSection.locator('.hero__logo img');
    await expect(logo).toBeVisible();

    // Verify logo has proper dimensions
    const logoBox = await logo.boundingBox();
    expect(logoBox).not.toBeNull();
    expect(logoBox.width).toBeGreaterThan(0);
    expect(logoBox.height).toBeGreaterThan(0);

    // Verify MiRDB title is visible
    const title = heroSection.locator('.hero__title');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('MiRDB');
  });

  test('TC2: Value proposition contains key phrases about product purpose', async ({ page }) => {
    // Check value proposition text content
    const heroSection = page.locator('#hero');
    const tagline = heroSection.locator('.hero__tagline');

    await expect(tagline).toBeVisible();

    // Verify text contains key phrases about MiRDB's purpose
    const taglineText = await tagline.textContent();
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('key-value');

    // Check for memcached protocol mention
    expect(taglineText.toLowerCase()).toContain('memcached protocol');
  });

  test('TC3: View on GitHub button opens repository in new tab', async ({ page, context }) => {
    // Click 'View on GitHub' button
    const heroSection = page.locator('#hero');
    const githubButton = heroSection.locator('a.btn-primary:has-text("View on GitHub")');

    await expect(githubButton).toBeVisible();

    // Verify button has correct href
    const href = await githubButton.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify button opens in new tab
    const target = await githubButton.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await githubButton.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC4: Get Started button navigates to quick start section', async ({ page }) => {
    // Click 'Get Started' button
    const heroSection = page.locator('#hero');
    const getStartedButton = heroSection.locator('a.btn-secondary:has-text("Get Started")');

    await expect(getStartedButton).toBeVisible();

    // Verify button has correct href pointing to quickstart section
    const href = await getStartedButton.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Click and verify scroll/navigation
    await getStartedButton.click();

    // Check that URL hash is updated
    await expect(page).toHaveURL(/#quickstart/);
  });

  test('Hero section is visible and properly structured', async ({ page }) => {
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();
    await expect(hero).toHaveClass(/hero/);

    // Verify all required elements exist
    await expect(hero.locator('.hero__logo')).toBeVisible();
    await expect(hero.locator('.hero__title')).toBeVisible();
    await expect(hero.locator('.hero__tagline')).toBeVisible();
    await expect(hero.locator('.hero__cta')).toBeVisible();
  });

  test('Hero section has proper accessibility attributes', async ({ page }) => {
    const hero = page.locator('#hero');

    // Check aria-labelledby points to the title
    await expect(hero).toHaveAttribute('aria-labelledby', 'hero-title');

    // Verify GitHub button has accessible label
    const githubButton = hero.locator('a.btn-primary');
    const ariaLabel = await githubButton.getAttribute('aria-label');
    expect(ariaLabel).toContain('GitHub');
    expect(ariaLabel).toContain('new tab');
  });
});
